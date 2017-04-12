package didclab.cse.buffalo.hysterisis;

import didclab.cse.buffalo.ConfigurationParams;
import didclab.cse.buffalo.CooperativeChannels;
import didclab.cse.buffalo.Partition;
import didclab.cse.buffalo.utils.TunableParameters;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class Hysterisis {

  private static final Log LOG = LogFactory.getLog(Hysterisis.class);
  private static List<List<Entry>> entries;
  private double[] estimatedThroughputs;
  private int[][] estimatedParamsForChunks;

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
      Similarity.categorizeEntries(similarEntries, chunks.get(chunkNumber).getDensity().name());
      setCounts[chunkNumber] = trials.size();
      //LOG.info("Chunk "+chunkNumber + " entries are categorized and written to disk at "+ jvmUpTime);
    }
  }

  public static double[] runModelling(Partition chunk, TunableParameters tunableParameters, double sampleThroughput,
                                      double[] relaxation_rates) {
    double []resultValues = new double[4];
    try {
      double sampleThroughputinMb = sampleThroughput / Math.pow(10, 6);
      ProcessBuilder pb = new ProcessBuilder("python", "src/main/python/optimizer.py",
          "-f", "chunk_"+chunk.getDensity()+".txt",
          "-c", "" + tunableParameters.getConcurrency(),
          "-p", "" + tunableParameters.getParallelism(),
          "-q", ""+ tunableParameters.getPipelining(),
          "-t", "" + sampleThroughputinMb,
          "--cc-rate" , ""+ relaxation_rates[0],
          "--p-rate" , ""+ relaxation_rates[1],
          "--ppq-rate" , ""+ relaxation_rates[2],
          "--maxcc", "" + CooperativeChannels.intendedTransfer.getMaxConcurrency());
      String formatedString = pb.command().toString()
          .replace(",", "")  //remove the commas
          .replace("[", "")  //remove the right bracket
          .replace("]", "")  //remove the left bracket
          .trim();           //remove trailing spaces from partially initialized arrays
      System.out.println("input:" + formatedString);
      Process p = pb.start();

      BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
      String output, line;
      if ((output = in.readLine()) == null) {
        in = new BufferedReader(new InputStreamReader(p.getErrorStream()));
        while(( output = in.readLine()) !=  null){
          System.out.println("Output:" + output);
        }
      }
      while ((line = in.readLine()) != null){ // Ignore intermediate log messages
        output = line;
      }
      String []values = output.trim().split("\\s+");
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
