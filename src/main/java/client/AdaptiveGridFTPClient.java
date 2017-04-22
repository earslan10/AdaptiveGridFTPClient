package client;


import client.hysterisis.Entry;
import client.hysterisis.Hysterisis;
import client.log.LogManager;
import client.utils.Utils;
import client.utils.Utils.Density;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import stork.module.CooperativeModule.GridFTPTransfer;
import stork.util.XferList;

import java.io.*;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class AdaptiveGridFTPClient {

  private static final Log LOG = LogFactory.getLog(AdaptiveGridFTPClient.class);
  public static Entry transferTask;
  public static boolean useOnlineTuning = false;
  public static boolean isTransferCompleted = false;
  TransferAlgorithm algorithm = TransferAlgorithm.MULTICHUNK;
  int maximumChunks = 4;
  private String proxyFile;
  private ChannelDistributionPolicy channelDistPolicy = ChannelDistributionPolicy.WEIGHTED;
  private boolean anonymousTransfer = false;
  private Hysterisis hysterisis;
  private GridFTPTransfer gridFTPClient;
  private boolean useHysterisis = false;
  private boolean useDynamicScheduling = false;
  private boolean runChecksumControl = false;


  public AdaptiveGridFTPClient() {
    // TODO Auto-generated constructor stub
    //initialize output streams for message logging
    LogManager.createLogFile(ConfigurationParams.STDOUT_ID);
    LogManager.createLogFile(ConfigurationParams.INFO_LOG_ID);
    transferTask = new Entry();
  }

  @VisibleForTesting
  public AdaptiveGridFTPClient(GridFTPTransfer gridFTPClient) {
    this.gridFTPClient = gridFTPClient;
    LogManager.createLogFile(ConfigurationParams.STDOUT_ID);
    //LogManager.createLogFile(ConfigurationParams.INFO_LOG_ID);
  }

  public static void main(String[] args) throws Exception {
    AdaptiveGridFTPClient multiChunk = new AdaptiveGridFTPClient();
    multiChunk.parseArguments(args, multiChunk);
    multiChunk.transfer();
  }

  @VisibleForTesting
  void setUseHysterisis(boolean bool) {
    useHysterisis = bool;
  }

  @VisibleForTesting
  void transfer() throws Exception {
    transferTask.setBDP((transferTask.getBandwidth() * transferTask.getRtt()) / 8); // In MB
    String mHysterisis = useHysterisis ? "Hysterisis" : "";
    String mDynamic = useDynamicScheduling ? "Dynamic" : "";
    LOG.info("*************" + algorithm.name() + "************");
    //LogManager.writeToLog("*************" + algorithm.name() + "-" + mHysterisis + "-" + mDynamic + "************" + transferTask.getMaxConcurrency(), ConfigurationParams.INFO_LOG_ID);

    URI su = null, du = null;
    try {
      su = new URI(transferTask.getSource()).normalize();
      du = new URI(transferTask.getDestination()).normalize();
    } catch (URISyntaxException e) {
      e.printStackTrace();
      System.exit(-1);
    }

    HostResolution sourceHostResolution = new HostResolution(su.getHost());
    HostResolution destinationHostResolution = new HostResolution(du.getHost());
    sourceHostResolution.start();
    destinationHostResolution.start();

    // create Control Channel to source and destination server
    double startTime = System.currentTimeMillis();
    if (gridFTPClient == null) {
      gridFTPClient = new GridFTPTransfer(proxyFile, su, du);
      gridFTPClient.start();
      gridFTPClient.waitFor();
    }
    if (gridFTPClient == null || GridFTPTransfer.client == null) {
      LOG.info("Could not establish GridFTP connection. Exiting...");
      System.exit(-1);
    }
    gridFTPClient.useDynamicScheduling = useDynamicScheduling;
    GridFTPTransfer.client.setChecksumEnabled(runChecksumControl);
    if (useHysterisis) {
      // this will initialize matlab connection while running hysterisis analysis
      hysterisis = new Hysterisis();
    }

    //Get metadata information of dataset
    XferList dataset = gridFTPClient.getListofFiles(su.getPath(), du.getPath());
    LOG.info("mlsr completed at:" + ((System.currentTimeMillis() - startTime) / 1000.0) + "set size:" + dataset.size());


    long datasetSize = dataset.size();
    ArrayList<Partition> chunks = partitionByFileSize(dataset, maximumChunks);

    // Make sure hostname resolution operations are completed before starting to a transfer
    sourceHostResolution.join();
    destinationHostResolution.join();
    for (InetAddress inetAddress : sourceHostResolution.allIPs) {
      if (inetAddress != null)
        gridFTPClient.sourceIpList.add(inetAddress);
    }
    for (InetAddress inetAddress : destinationHostResolution.allIPs) {
      if (inetAddress != null )
        gridFTPClient.destinationIpList.add(inetAddress);
    }

    int[][] estimatedParamsForChunks = new int[chunks.size()][4];
    long timeSpent = 0;
    long start = System.currentTimeMillis();
    if (useHysterisis) {
      hysterisis.findOptimalParameters(chunks, transferTask);
    }
    gridFTPClient.startTransferMonitor();
    switch (algorithm) {
      case SINGLECHUNK:
        chunks.forEach(chunk -> chunk.setTunableParameters(Utils.getBestParams(chunk.getRecords(), maximumChunks)));
        GridFTPTransfer.executor.submit(new GridFTPTransfer.ModellingThread());
        chunks.forEach(chunk -> gridFTPClient.runTransfer(chunk));

        break;
      default:
        // Make sure total channels count does not exceed total file count
        int totalChannelCount = Math.min(transferTask.getMaxConcurrency(), dataset.count());
        if (useHysterisis) {
          int maxConcurrency = 0;
          for (int i = 0; i < estimatedParamsForChunks.length; i++) {
            chunks.get(i).getRecords().setTransferParameters(estimatedParamsForChunks[i]);
            if (estimatedParamsForChunks[i][0] > maxConcurrency) {
              maxConcurrency = estimatedParamsForChunks[i][0];
            }
          }
          LOG.info(" Running MC with :" + maxConcurrency + " channels.");
          totalChannelCount = maxConcurrency;
        } else {
          for (int i = 0; i < estimatedParamsForChunks.length; i++) {
            chunks.get(i).setTunableParameters(Utils.getBestParams(chunks.get(i).getRecords(), maximumChunks));
          }
        }
        LOG.info(" Running MC with :" + totalChannelCount + " channels.");
        int[] channelAllocation = allocateChannelsToChunks(chunks, totalChannelCount);
        start = System.currentTimeMillis();
        gridFTPClient.runMultiChunkTransfer(chunks, channelAllocation);
        break;
    }
    timeSpent += ((System.currentTimeMillis() - start) / 1000.0);
    LogManager.writeToLog("Chunks: " + maximumChunks + " Throughput:" +
        (datasetSize * 8.0) / (timeSpent * (1000.0 * 1000)), ConfigurationParams.INFO_LOG_ID);
    System.out.println("Chunks: " + maximumChunks + " Throughput:" + "size:" + Utils.printSize(datasetSize, true)
        + " time:" + timeSpent + " thr: "+ (datasetSize * 8.0) / (timeSpent * (1000.0 * 1000)));

    isTransferCompleted = true;
    GridFTPTransfer.executor.shutdown();
    while (!GridFTPTransfer.executor.isTerminated()) {
    }
    LogManager.close();
    gridFTPClient.stop();
  }

  @VisibleForTesting
  ArrayList<Partition> partitionByFileSize(XferList list, int maximumChunks) {
    Entry transferTask = getTransferTask();
    list.shuffle();

    ArrayList<Partition> partitions = new ArrayList<>();
    for (int i = 0; i < maximumChunks; i++) {
      Partition p = new Partition();
      partitions.add(p);
    }
    for (XferList.MlsxEntry e : list) {
      if (e.dir) {
        continue;
      }
      Density density = Utils.findDensityOfFile(e.size(), transferTask.getBandwidth(), maximumChunks);
      partitions.get(density.ordinal()).addRecord(e);
    }
    Collections.sort(partitions);
    mergePartitions(partitions);

    for (int i = 0; i < partitions.size(); i++) {
      Partition chunk = partitions.get(i);
      chunk.getRecords().sp = list.sp;
      chunk.getRecords().dp = list.dp;
      double avgFileSize = chunk.getRecords().size() / (chunk.getRecords().count() * 1.0);
      chunk.setEntry(transferTask);
      chunk.entry.setFileSize(avgFileSize);
      chunk.entry.setFileCount(chunk.getRecords().count());
      chunk.entry.setDensity(Entry.findDensityOfList(avgFileSize, transferTask.getBandwidth()));
      chunk.entry.calculateSpecVector();
      chunk.setDensity(Entry.findDensityOfList(avgFileSize, transferTask.getBandwidth()));
      LOG.info("Chunk " + i + ":\tfiles:" + partitions.get(i).getRecords().count() + "\t avg:" +
          Utils.printSize(partitions.get(i).getCentroid(), true)
          + "\t" + Utils.printSize(partitions.get(i).getRecords().size(), true) + " Density:" +
          chunk.getDensity());
    }
    return partitions;
  }

  private ArrayList<Partition> mergePartitions(ArrayList<Partition> partitions) {
    for (int i = 0; i < partitions.size(); i++) {
      Partition p = partitions.get(i);
      //merge small chunk with the the chunk with closest centroid
      if ((p.getRecords().count() <= 2 || p.getRecords().size() < 5 * transferTask.getBDP()) && partitions.size() > 1) {
        int index = -1;
        double diff = Double.POSITIVE_INFINITY;
        for (int j = 0; j < partitions.size(); j++) {
          if (j != i && Math.abs(p.getCentroid() - partitions.get(j).getCentroid()) < diff) {
            diff = Math.abs(p.getCentroid() - partitions.get(j).getCentroid());
            index = j;
          }
        }
        if (index == -1) {
          LOG.fatal("Fatal error: Could not find chunk to merge!");
          System.exit(-1);
        }
        partitions.get(index).getRecords().addAll(p.getRecords());
        LOG.info("Partition " + i + " " + p.getRecords().count() + " files " +
            Utils.printSize(p.getRecords().size(), true));
        LOG.info("Merging partition " + i + " to partition " + index);
        partitions.remove(i);
        i--;
      }
    }
    return partitions;
  }

  private int[] allocateChannelsToChunks(List<Partition> chunks, final int channelCount) {
    int totalChunks = chunks.size();
    int[] fileCount = new int[totalChunks];
    for (int i = 0; i < totalChunks; i++) {
      fileCount[i] = chunks.get(i).getRecords().count();
    }

    int[] concurrencyLevels = new int[totalChunks];
    if (channelDistPolicy == ChannelDistributionPolicy.ROUND_ROBIN) {
      int modulo = (totalChunks + 1) / 2;
      int count = 0;
      for (int i = 0; count < channelCount; i++) {
        int index = i % modulo;
        if (concurrencyLevels[index] < fileCount[index]) {
          concurrencyLevels[index]++;
          count++;
        }
        if (index < totalChunks - index - 1 && count < channelCount
            && concurrencyLevels[totalChunks - index - 1] < fileCount[totalChunks - index - 1]) {
          concurrencyLevels[totalChunks - index - 1]++;
          count++;
        }
      }

      for (int i = 0; i < totalChunks; i++) {
        System.out.println("Chunk " + i + ":" + concurrencyLevels[i] + "channels");
      }
    } else {
      double[] chunkWeights = new double[chunks.size()];
      double totalWeight = 0;
      if (useHysterisis) {
        /*
        int[][] estimatedParams = hysterisis.getEstimatedParams();
        double[] estimatedThroughputs = hysterisis.getEstimatedThroughputs();
        double[] estimatedUnitThroughputs = new double[chunks.size()];
        for (int i = 0; i < chunks.size(); i++) {
          estimatedUnitThroughputs[i] = estimatedThroughputs[i] / estimatedParams[i][0];
          LOG.info("estimated unit thr:" + estimatedUnitThroughputs[i] + " " + estimatedParams[i][0]);
        }
        double totalThroughput = 0;
        for (double estimatedUnitThroughput : estimatedUnitThroughputs) {
          totalThroughput += estimatedUnitThroughput;
        }
        for (int i = 0; i < estimatedThroughputs.length; i++) {
          chunkWeights[i] = chunks.get(i).getTotalSize() * (totalThroughput / estimatedUnitThroughputs[i]);
          totalWeight += chunkWeights[i];
        }
        */
      } else {
        double[] chunkSize = new double[chunks.size()];
        for (int i = 0; i < chunks.size(); i++) {
          chunkSize[i] = chunks.get(i).getTotalSize();
          Utils.Density densityOfChunk = chunks.get(i).getDensity();
          switch (densityOfChunk) {
            case SMALL:
              chunkWeights[i] = 6 * chunkSize[i];
              break;
            case MEDIUM:
              chunkWeights[i] = 3 * chunkSize[i];
              break;
            case LARGE:
              chunkWeights[i] = 2 * chunkSize[i];
              break;
            case HUGE:
              chunkWeights[i] = 1 * chunkSize[i];
              break;
            default:
              break;
          }
          totalWeight += chunkWeights[i];
        }
      }
      int remainingChannelCount = channelCount;

      for (int i = 0; i < totalChunks; i++) {
        double propChunkWeight = (chunkWeights[i] * 1.0 / totalWeight);
        concurrencyLevels[i] = Math.min(remainingChannelCount, (int) Math.floor(channelCount * propChunkWeight));
        remainingChannelCount -= concurrencyLevels[i];
        //System.out.println("Channel "+i + "weight:" +propChunkWeight  + "got " + concurrencyLevels[i] + "channels");
      }

      // Since we take floor when calculating, total channels might be unassigned.
      // If so, starting from chunks with zero channels, assign remaining channels
      // in round robin fashion
      for (int i = 0; i < chunks.size(); i++) {
        if (concurrencyLevels[i] == 0 && remainingChannelCount > 0) {
          concurrencyLevels[i]++;
          remainingChannelCount--;
        }
      }
      //find the chunks with minimum assignedChannelCount
      while (remainingChannelCount > 0) {
        int minChannelCount = Integer.MAX_VALUE;
        int chunkIdWithMinChannel = -1;
        for (int i = 0; i < chunks.size(); i++) {
          if (concurrencyLevels[i] < minChannelCount) {
            minChannelCount = concurrencyLevels[i];
            chunkIdWithMinChannel = i;
          }
        }
        concurrencyLevels[chunkIdWithMinChannel]++;
        remainingChannelCount--;
      }
      for (int i = 0; i < totalChunks; i++) {
        Partition chunk = chunks.get(i);
        LOG.info("Chunk " + chunk.getDensity() + " weight " + chunkWeights[i] + " cc: " + concurrencyLevels[i]);
      }
    }
    return concurrencyLevels;
  }

  public Entry getTransferTask() {
    return transferTask;
  }

  private void parseArguments(String[] arguments, AdaptiveGridFTPClient multiChunk) {
    ClassLoader classloader = Thread.currentThread().getContextClassLoader();
    String configFile = "config.cfg";
    InputStream is;
    try {
      if (arguments.length > 0) {
        is = new FileInputStream(arguments[0]);
      } else {
        is = classloader.getResourceAsStream(configFile);
      }

      BufferedReader br = new BufferedReader(new InputStreamReader(is));
      String line;
      while ((line = br.readLine()) != null) {
        processParameter(line.split("\\s+"));
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    for (int i = 1; i < arguments.length; i++) {
      String argument = arguments[i];
      if (i + 1 < arguments.length) {
        String value = arguments[i + 1];
        if (processParameter(argument, value))
          i++;
      } else
        processParameter(argument);
    }

    if (null == proxyFile && !anonymousTransfer) {
      int uid = findUserId();
      proxyFile = "/tmp/x509up_u" + uid;
    }
    ConfigurationParams.init();
  }

  private boolean processParameter(String... args) {
    String config = args[0];
    boolean usedSecondArgument = true;
    if (config.startsWith("#")) {
      return !usedSecondArgument;
    }
    switch (config) {
      case "-s":
      case "-source":
        if (args.length > 1) {
          transferTask.setSource(args[1]);
        } else {
          LOG.fatal("-source requires source address");
        }
        LOG.info("source  = " + transferTask.getSource());
        break;
      case "-d":
      case "-destination":
        if (args.length > 1) {
          transferTask.setDestination(args[1]);
        } else {
          LOG.fatal("-destination requires a destination address");
        }
        LOG.info("destination = " + transferTask.getDestination());
        break;
      case "-proxy":
        if (args.length > 1) {
          proxyFile = args[1];
        } else {
          LOG.fatal("-spath requires spath of file/directory to be transferred");
        }
        LOG.info("proxyFile = " + proxyFile);
        break;
      case "-no-proxy":
        anonymousTransfer = true;
        break;
      case "-bw":
      case "-bandwidth":
        if (args.length > 1 || Double.parseDouble(args[1]) > 100) {
          transferTask.setBandwidth(Math.pow(10, 9) * Double.parseDouble(args[1]));
        } else {
          LOG.fatal("-bw requires bandwidth in GB");
        }
        LOG.info("bandwidth = " + transferTask.getBandwidth() + " GB");
        break;
      case "-rtt":
        if (args.length > 1) {
          transferTask.setRtt(Double.parseDouble(args[1]));
        } else {
          LOG.fatal("-rtt requires round trip time in millisecond");
        }
        LOG.info("rtt = " + transferTask.getRtt() + " ms");
        break;
      case "-maxcc":
      case "-max-concurrency":
        if (args.length > 1) {
          transferTask.setMaxConcurrency(Integer.parseInt(args[1]));
        } else {
          LOG.fatal("-cc needs integer");
        }
        LOG.info("cc = " + transferTask.getMaxConcurrency());
        break;
      case "-bs":
      case "-buffer-size":
        if (args.length > 1) {
          transferTask.setBufferSize(Integer.parseInt(args[1]) * 1024 * 1024); //in MB
        } else {
          LOG.fatal("-bs needs integer");
        }
        LOG.info("bs = " + transferTask.getBufferSize());
        break;
      case "-testbed":
        if (args.length > 1) {
          transferTask.setTestbed(args[1]);
        } else {
          LOG.fatal("-testbed needs testbed name");
        }
        LOG.info("Testbed name is = " + transferTask.getTestbed());
        break;
      case "-input":
        if (args.length > 1) {
          ConfigurationParams.INPUT_DIR = args[1];
        } else {
          LOG.fatal("-historical data input file spath has to be passed");
        }
        LOG.info("Historical data spath = " + ConfigurationParams.INPUT_DIR);
        break;
      case "-maximumChunks":
        if (args.length > 1) {
          maximumChunks = Integer.parseInt(args[1]);
        } else {
          LOG.fatal("-maximumChunks requires an integer value");
        }
        LOG.info("Number of chunks = " + maximumChunks);
        break;
      case "-use-hysterisis":
        useHysterisis = true;
        LOG.info("Use hysterisis based approach");
        break;
      case "-single-chunk":
        algorithm = TransferAlgorithm.SINGLECHUNK;
        LOG.info("Use single chunk transfer approach");
        break;
      case "-channel-distribution-policy":
        if (args.length > 1) {
          if (args[1].compareTo("roundrobin") == 0) {
            channelDistPolicy = ChannelDistributionPolicy.ROUND_ROBIN;
          } else if (args[1].compareTo("weighted") == 0) {
            channelDistPolicy = ChannelDistributionPolicy.WEIGHTED;
          } else {
            LOG.fatal("-channel-distribution-policy can be either \"roundrobin\" or \"weighted\"");
          }
        } else {
          LOG.fatal("-channel-distribution-policy has to be specified as \"roundrobin\" or \"weighted\"");
        }
        break;
      case "-use-dynamic-scheduling":
        useDynamicScheduling = true;
        LOG.info("Dynamic scheduling enabled.");
        break;
      case "-use-online-tuning":
        useOnlineTuning = true;
        LOG.info("Online modelling/tuning enabled.");
        break;
      case "-use-checksum":
        runChecksumControl = true;
        LOG.info("Dynamic scheduling enabled.");
        break;
      default:
        System.err.println("Unrecognized input parameter " + config);
        System.exit(-1);
    }
    return usedSecondArgument;
  }

  private int findUserId() {
    String userName = null;
    int uid = -1;
    try {
      userName = System.getProperty("user.name");
      String command = "id -u " + userName;
      Process child = Runtime.getRuntime().exec(command);
      BufferedReader stdInput = new BufferedReader(new InputStreamReader(child.getInputStream()));
      String s;
      while ((s = stdInput.readLine()) != null) {
        uid = Integer.parseInt(s);
      }
      stdInput.close();
    } catch (IOException e) {
      System.err.print("Proxy file for user " + userName + " not found!" + e.getMessage());
      System.exit(-1);
    }
    return uid;
  }

  public static class HostResolution extends Thread {
    String hostname;
    InetAddress[] allIPs;

    public HostResolution(String hostname) {
      this.hostname = hostname;
    }

    @Override
    public void run() {
      try {
        System.out.println("Getting IPs for:" + hostname);
        allIPs = InetAddress.getAllByName(hostname);
        System.out.println("Found IPs for:" + hostname);
        //System.out.println("IPs for:" + du.getHost());
        for (int i = 0; i < allIPs.length; i++) {
          System.out.println(allIPs[i]);
        }
      } catch (Exception e) {
        e.printStackTrace();
        System.exit(-1);
      }
    }

  }

  enum TransferAlgorithm {SINGLECHUNK, MULTICHUNK}

  private enum ChannelDistributionPolicy {ROUND_ROBIN, WEIGHTED}


}
