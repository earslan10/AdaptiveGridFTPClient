package didclab.cse.buffalo.hysterisis;

import didclab.cse.buffalo.ConfigurationParams;
import didclab.cse.buffalo.Partition;
import didclab.cse.buffalo.utils.Utils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stork.module.CooperativeModule.GridFTPTransfer;
import stork.util.XferList;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class Hysterisis {

  private static final Log LOG = LogFactory.getLog(Hysterisis.class);
  private static List<List<Entry>> entries;
  public double optimizationAlgorithmTime = 2;
  private int[][] estimatedParamsForChunks;
  private GridFTPTransfer gridFTPClient;
  private double[] estimatedThroughputs;

  public Hysterisis(GridFTPTransfer gridFTPClient) {
    // TODO Auto-generated constructor stub
    this.gridFTPClient = gridFTPClient;
  }


  private void parseInputFiles() {
    File folder = new File(ConfigurationParams.INPUT_DIR);
    if (!folder.exists()) {
      LOG.error("Cannot access to " + folder.getAbsoluteFile());
      System.exit(-1);
    }
    File[] listOfFiles = folder.listFiles();
    if (listOfFiles.length == 0) {
      LOG.error("No historical data found at " + folder.getAbsoluteFile());
      System.exit(-1);
    }
    List<String> historicalDataset = new ArrayList<>(listOfFiles.length);
    entries = new ArrayList<>();
    for (File listOfFile : listOfFiles) {
      if (listOfFile.isFile()) {
        historicalDataset.add(ConfigurationParams.INPUT_DIR + listOfFile.getName());
      }
    }
    for (String fileName : historicalDataset) {
      List<Entry> fileEntries = Similarity.readFile(fileName);
      if (!fileEntries.isEmpty()) {
        entries.add(fileEntries);
      }
    }
    //System.out.println("Skipped Entry count =" + Similarity.skippedEntryCount);
  }

  public int[][] findOptimalParameters(List<Partition> chunks,
                                       Entry intendedTransfer, XferList dataset) throws Exception {
    parseInputFiles();
    if (entries.isEmpty()) {  // Make sure there are log files to run hysterisis
      LOG.fatal("No input entries found to run hysterisis analysis. Exiting...");
    }

    // Find out similar entries
    int[] setCounts = new int[chunks.size()];
    Similarity.normalizeDataset3(entries, chunks);
    //LOG.info("Entries are normalized at "+ ManagementFactory.getRuntimeMXBean().getUptime());
    for (int chunkNumber = 0; chunkNumber < chunks.size(); chunkNumber++) {
      List<Entry> similarEntries = Similarity.findSimilarEntries(entries, chunks.get(chunkNumber).entry);
      //Categorize selected entries based on log date
      List<List<Entry>> trials = new LinkedList<>();
      Similarity.categorizeEntries(chunkNumber, trials, similarEntries);
      setCounts[chunkNumber] = trials.size();
      //LOG.info("Chunk "+chunkNumber + " entries are categorized and written to disk at "+ jvmUpTime);
    }

    double[] sampleThroughputs = new double[chunks.size()];
    // Run sample transfer to learn current network load
    // Create sample dataset to and transfer to measure current load on the network
    for (int chunkNumber = 0; chunkNumber < chunks.size(); chunkNumber++) {
      Partition chunk = chunks.get(chunkNumber);
      long SAMPLING_SIZE = (int) (intendedTransfer.getBandwidth() / 4);
      //System.out.println(chunk.getRecords().count() +" "+ chunk.getRecords().size()  + "s:" +SAMPLING_SIZE);
      XferList sample_files = chunk.getRecords().split(SAMPLING_SIZE);
      //System.out.println("Sample transfer list:" + sample_files.count() +" "+ sample_files.size()
      //	+" original list:" +chunk.getRecords().count() +" "+ chunk.getRecords().size() );
      int[] samplingParams = Utils.getBestParams(sample_files);
      // use higher concurrency values for sampling to be able to
      // observe available throughput better
      int cc = samplingParams[0];
      cc = cc > 8 ? cc : Math.min(8, sample_files.count());
      samplingParams[0] = cc;
      chunk.setSamplingSize(sample_files.size());
      chunk.setSamplingParameters(samplingParams);
      long start = System.currentTimeMillis();
      //LOG.info("Sample transfer called at "+ManagementFactory.getRuntimeMXBean().getUptime());
      sampleThroughputs[chunkNumber] = gridFTPClient.runTransfer(samplingParams[0], samplingParams[1],
              samplingParams[2], samplingParams[3], sample_files, chunkNumber);
      chunk.setSamplingTime((System.currentTimeMillis() - start) / 1000.0);
      // LOG.info( chunk.getSamplingTime() + " "+  chunk.getSamplingSize());
      // TODO: handle transfer failure
      if (sampleThroughputs[chunkNumber] == -1) {
        System.exit(-1);
      }
    }
    // Based on input files and sample tranfer throughput; categorize logs and fit model
    // Then find optimal parameter values out of the model
    double[][] results = runModelling(chunks, sampleThroughputs);
    if (results == null) {
      return null;
    }
    estimatedParamsForChunks = new int[chunks.size()][4];
    estimatedThroughputs = new double[chunks.size()];
    for (int i = 0; i < results.length; i++) {
      double[] paramValues = results[i];
      //double[] paramValues =  result[0];
      //Convert from double to int
      int[] parameters = new int[paramValues.length];
      for (int j = 0; j < paramValues.length-1; j++)
        parameters[j] = (int) paramValues[j];
      parameters[paramValues.length-1] = (int) intendedTransfer.getBufferSize();
      estimatedParamsForChunks[i] = parameters;
      //estimatedAccuracies[i] = ((double[]) result[2])[0];
      estimatedThroughputs[i] = paramValues[paramValues.length-1];
      LOG.info("Estimated params cc:" + paramValues[0] + " p:" + paramValues[1] +
              " ppq:" + paramValues[2] + " throughput: " + estimatedThroughputs[i]);
    }
    return estimatedParamsForChunks;
  }

  public double [][] runModelling(List<Partition> chunks, double[] sampleThroughputs) {
    double [][]outputList = new double[chunks.size()][4];
    try{
      for (int chunkNumber = 0; chunkNumber < chunks.size(); chunkNumber++) {
        int[] sampleTransferValues = chunks.get(chunkNumber).getSamplingParameters();
        double sampleThroughputinMb = sampleThroughputs[chunkNumber] / Math.pow(10, 6);
        ProcessBuilder pb = new ProcessBuilder("python", "src/main/python/optimizer.py",
                "-f", "chunk_"+chunkNumber+".txt",
                "-c", "" + sampleTransferValues[0],
                "-p", "" + sampleTransferValues[1],
                "-q", ""+sampleTransferValues[2],
                "-t", "" + sampleThroughputinMb);
        System.out.println("input:" + pb.command());
        Process p = pb.start();

        BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
        String output = in.readLine();
        if (output == null) {
          in = new BufferedReader(new InputStreamReader(p.getErrorStream()));
          while(( output = in.readLine()) !=  null){
            System.out.println("Output:" + output);
          }
        }
        String []values = output.replaceAll("\\[", "").replaceAll("\\]", "").trim().split("\\s+", -1);
        for (int i = 0; i < values.length; i++) {
          outputList[chunkNumber][i] = Double.parseDouble(values[i]);
        }

      }
    }catch(Exception e) {
      System.out.println(e);
    }
    return outputList;
}

  public double[] getEstimatedThroughputs() {
    return estimatedThroughputs;
  }

  public int[][] getEstimatedParams() {
    return estimatedParamsForChunks;
  }

}
