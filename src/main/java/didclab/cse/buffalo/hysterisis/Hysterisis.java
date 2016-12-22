package didclab.cse.buffalo.hysterisis;

import didclab.cse.buffalo.ConfigurationParams;
import didclab.cse.buffalo.Partition;
import didclab.cse.buffalo.utils.TunableParameters;
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
import java.util.concurrent.*;

public class Hysterisis {

  private static final Log LOG = LogFactory.getLog(Hysterisis.class);
  private static List<List<Entry>> entries;
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

  public void findOptimalParameters(List<Partition> chunks, Entry intendedTransfer) throws Exception {
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

    gridFTPClient.executor.submit(new GridFTPTransfer.ModellingThread());
    double[] sampleThroughputs = new double[chunks.size()];
    // Run sample transfer to learn current network load
    // Create sample dataset to and transfer to measure current load on the network
    GridFTPTransfer.ModellingThread thread = null;
    for (int chunkNumber = 0; chunkNumber < chunks.size(); chunkNumber++) {
      Partition chunk = chunks.get(chunkNumber);
      long SAMPLING_SIZE = (int) (intendedTransfer.getBandwidth() / 4);
      //System.out.println(chunk.getRecords().count() +" "+ chunk.getRecords().size()  + "s:" +SAMPLING_SIZE);
      XferList sample_files = chunk.getRecords().split(SAMPLING_SIZE);

      Partition sampleChunk = new Partition();
      sampleChunk.setXferList(sample_files);
      sampleChunk.setChunkNumber(chunk.getChunkNumber());
      //System.out.println("Sample transfer list:" + sample_files.count() +" "+ sample_files.size()
      //	+" original list:" +chunk.getRecords().count() +" "+ chunk.getRecords().size() );
      sampleChunk.setTunableParameters(Utils.getBestParams(sample_files));
      // use higher concurrency values for sampling to be able to
      // observe available throughput better
      int cc = sampleChunk.getTunableParameters().getConcurrency();
      cc = cc > 8 ? cc : Math.min(8, sample_files.count());
      sampleChunk.getTunableParameters().setConcurrency(cc);

      long start = System.currentTimeMillis();
      sampleThroughputs[chunkNumber] = gridFTPClient.runTransfer(sampleChunk);
      chunk.setSamplingTime((System.currentTimeMillis() - start) / 1000.0);

      GridFTPTransfer.ModellingThread.jobQueue.add(new GridFTPTransfer.ModellingThread.ModellingJob(
          chunk, sampleChunk.getTunableParameters(), sampleThroughputs[chunkNumber]));
      // LOG.info( chunk.getSamplingTime() + " "+  chunk.getSamplingSize());
      // TODO: handle transfer failure
      if (sampleThroughputs[chunkNumber] == -1) {
        System.exit(-1);
      }
    }
    // Make sure last chunk modelling finished before going on.
    while (!GridFTPTransfer.ModellingThread.jobQueue.isEmpty()) {
      Thread.sleep(1000);
    }
  }

  public static double[] runModelling(Partition chunk, TunableParameters tunableParameters, double sampleThroughput) {
    double []resultValues = new double[4];
    try {
      double sampleThroughputinMb = sampleThroughput / Math.pow(10, 6);
      ProcessBuilder pb = new ProcessBuilder("python", "src/main/python/optimizer.py",
          "-f", "chunk_"+chunk.getChunkNumber()+".txt",
          "-c", "" + tunableParameters.getConcurrency(),
          "-p", "" + tunableParameters.getParallelism(),
          "-q", ""+ tunableParameters.getPipelining(),
          "-t", "" + sampleThroughputinMb);
      //System.out.println("input:" + pb.command());
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
        resultValues[i] = Double.parseDouble(values[i]);
      }
    } catch(Exception e) {
      System.out.println(e);
    }
    return resultValues;
  }

  public double[] getEstimatedThroughputs() {
    return estimatedThroughputs;
  }

  public int[][] getEstimatedParams() {
    return estimatedParamsForChunks;
  }

}
