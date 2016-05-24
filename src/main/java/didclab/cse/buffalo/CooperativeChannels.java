package didclab.cse.buffalo;


import com.google.common.annotations.VisibleForTesting;
import didclab.cse.buffalo.hysterisis.Entry;
import didclab.cse.buffalo.hysterisis.Hysterisis;
import didclab.cse.buffalo.log.LogManager;
import didclab.cse.buffalo.utils.Utils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.ml.clustering.Cluster;
import org.apache.commons.math3.ml.clustering.DBSCANClusterer;
import org.apache.commons.math3.ml.clustering.DoublePoint;
import stork.module.CooperativeModule.GridFTPTransfer;
import stork.util.XferList;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class CooperativeChannels {

  private static final Log LOG = LogFactory.getLog(CooperativeChannels.class);
  public static Entry intendedTransfer;
  static double totalTransferTime = 0;
  TransferAlgorithm algorithm = TransferAlgorithm.MULTICHUNK;
  private String proxyFile;
  private ChannelDistributionPolicy channelDistPolicy = ChannelDistributionPolicy.WEIGHTED;
  private Hysterisis hysterisis;
  private GridFTPTransfer gridFTPClient;
  private boolean useHysterisis = false;
  private boolean useDynamicScheduling = false;

  public CooperativeChannels() {
    // TODO Auto-generated constructor stub
    //initialize output streams for message logging
    LogManager.createLogFile(ConfigurationParams.STDOUT_ID);
    LogManager.createLogFile(ConfigurationParams.INFO_LOG_ID);
    intendedTransfer = new Entry();
  }

  @VisibleForTesting
  public CooperativeChannels(GridFTPTransfer gridFTPClient) {
    this.gridFTPClient = gridFTPClient;
    LogManager.createLogFile(ConfigurationParams.STDOUT_ID);
    //LogManager.createLogFile(ConfigurationParams.INFO_LOG_ID);
  }

  public static void main(String[] args) throws Exception {
    CooperativeChannels multiChunk = new CooperativeChannels();
    multiChunk.parseArguments(args);
    multiChunk.transfer();
  }

  @VisibleForTesting
  void setUseHysterisis(boolean bool) {
    useHysterisis = bool;
  }

  @VisibleForTesting
  void transfer() throws Exception {
    intendedTransfer.setBDP((intendedTransfer.getBandwidth() * intendedTransfer.getRtt()) / 8); // In MB
    String mHysterisis = useHysterisis ? "Hysterisis" : "";
    String mDynamic = useDynamicScheduling ? "Dynamic" : "";
    LOG.info("*************" + algorithm.name() + "************");
    LogManager.writeToLog("*************" + algorithm.name() + "-" + mHysterisis + "-" + mDynamic + "************" + intendedTransfer.getMaxConcurrency(), ConfigurationParams.INFO_LOG_ID);

    URI su = null, du = null;
    try {
      su = new URI(intendedTransfer.getSource()).normalize();
      du = new URI(intendedTransfer.getDestination()).normalize();
    } catch (URISyntaxException e) {
      e.printStackTrace();
      System.exit(-1);
    }
    // create Control Channel to source and destination server
    double startTime = System.currentTimeMillis();
    if (gridFTPClient == null) {
      gridFTPClient = new GridFTPTransfer(proxyFile, su, du);
      gridFTPClient.start();
      gridFTPClient.waitFor();
    }
    if (gridFTPClient == null || gridFTPClient.client == null) {
      LOG.info("GridFTP client connection couldnot be established. Exiting...");
      System.exit(-1);
    }
    gridFTPClient.useDynamicScheduling = useDynamicScheduling;
    if (useHysterisis) {
      // this will initialize matlab connection while running hysterisis analysis
      hysterisis = new Hysterisis(gridFTPClient);
    }

    //Get metadata information of dataset
    XferList dataset = gridFTPClient.getListofFiles(su.getPath(), du.getPath());

    LOG.info("mlsr completed at:" + ((System.currentTimeMillis() - startTime) / 1000.0) + "set size:" + dataset.size() );

    DBSCANClusterer dbscan = new DBSCANClusterer(intendedTransfer.getBDP()/100.0, 2);
    List<Cluster<DoublePoint>> cluster = dbscan.cluster(getGPS(dataset));

    System.out.println(cluster.size() + "**" + intendedTransfer.getBDP());
    for(Cluster<DoublePoint> c: cluster){
      System.out.println(c.getPoints().get(0));
    }

    System.exit(-1);

    long datasetSize = dataset.size();
    ArrayList<Partition> chunks = partitionByFileSize(dataset);

    int[][] estimatedParamsForChunks = new int[chunks.size()][4];
    long timeSpent = 0;
    if (useHysterisis) {
      estimatedParamsForChunks = hysterisis.findOptimalParameters(chunks, intendedTransfer, dataset);
      for (int i = 0; i < chunks.size(); i++) {
        timeSpent += chunks.get(i).getSamplingTime();
        if (estimatedParamsForChunks[i][0] > chunks.get(i).getRecords().count()) {
          estimatedParamsForChunks[i][0] = chunks.get(i).getRecords().count();
        }
      }
      timeSpent += hysterisis.optimizationAlgorithmTime;
      //estimatedParamsForChunks[0][2] = 17;
      gridFTPClient.client.chunks.clear();
    }
    switch (algorithm) {
      case SINGLECHUNK:
        if (!useHysterisis) {
          for (int i = 0; i < chunks.size(); i++) {
            estimatedParamsForChunks[i] = Utils.getBestParams(chunks.get(i).getRecords());
          }
        }
        for (int i = 0; i < chunks.size(); i++) {
          int[] parameters = estimatedParamsForChunks[i];
          long start = System.currentTimeMillis();
          gridFTPClient.runTransfer(parameters[0], parameters[1],
                  parameters[2], parameters[3], chunks.get(i).getRecords(), i);
          timeSpent += ((System.currentTimeMillis() - start) / 1000.0);
        }
        break;
      default:
        int totalChannelCount = intendedTransfer.getMaxConcurrency();
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
            chunks.get(i).getRecords().setTransferParameters(Utils.getBestParams(chunks.get(i).getRecords()));
          }
        }
        LOG.info(" Running MC with :" + totalChannelCount + " channels.");
        int[] channelAllocation = allocateChannelsToChunks(chunks, totalChannelCount);
        long start = System.currentTimeMillis();
        gridFTPClient.runMultiChunkTransfer(chunks, channelAllocation);
        timeSpent += (System.currentTimeMillis() - start) / 1000.0;
        break;
    }
    LogManager.writeToLog("Final throughput:" + (datasetSize * 8.0) / (timeSpent * (1000.0 * 1000)), ConfigurationParams.INFO_LOG_ID);
    gridFTPClient.executor.shutdown();
    while (!gridFTPClient.executor.isTerminated()) {
    }
    LogManager.close();
    gridFTPClient.stop();
  }

  private static List<DoublePoint> getGPS(XferList list){

    List<DoublePoint> points = new ArrayList<DoublePoint>();
    double[] d = new double[1];
    int index = 0;
    Iterator<XferList.Entry> it = list.iterator();
    while (it.hasNext()) {
      XferList.Entry e =  it.next();
      System.out.println(e.path + "--" + e.size);
      d[0] = e.size;
      points.add(new DoublePoint(d));
    }
    //points.add(new DoublePoint(d));
    return points;
  }

  private ArrayList<Partition> mergePartitions(ArrayList<Partition> partitions) {
    for (int i = 0; i < partitions.size(); i++) {
      Partition p = partitions.get(i);
      if (p.getRecords().count() <= 2 || p.getRecords().size() < 5 * intendedTransfer.getBDP()) {  //merge small chunk with the the chunk with closest centroid
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
        LOG.info("Partition " + i + " " + p.getRecords().count() + " files " + Utils.printSize(p.getRecords().size(), true));
        LOG.info("Merging partition " + i + " to partition " + index);
        partitions.remove(i);
        i--;
      }
    }
    return partitions;
  }

  private ArrayList<Partition> partitionByFileSize(XferList list) {

    //remove folders from the list and send mkdir command to destination
    for (int i = 0; i < list.count(); i++) {
      if (list.getItem(i).dir) {
        list.removeItem(i);
      }
    }

    ArrayList<Partition> partitions = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      Partition p = new Partition();
      partitions.add(p);
    }
    for (XferList.Entry e : list) {
      double bandwidthInBytes = intendedTransfer.getBandwidth() / 8;
      if (e.size < bandwidthInBytes / 20) {
        partitions.get(0).addRecord(e);
      } else if (e.size < bandwidthInBytes / 5) {
        partitions.get(1).addRecord(e);
      } else if (e.size < bandwidthInBytes * 2) {
        partitions.get(2).addRecord(e);
      } else {
        partitions.get(3).addRecord(e);
      }
    }
    mergePartitions(partitions);
    for (int i = 0; i < partitions.size(); i++) {
      Partition chunk = partitions.get(i);
      chunk.getRecords().sp = list.sp;
      chunk.getRecords().dp = list.dp;
      double avgFileSize = chunk.getRecords().size() / (chunk.getRecords().count() * 1.0);
      chunk.setEntry(intendedTransfer);
      chunk.entry.setFileSize(avgFileSize);
      chunk.entry.setFileCount(chunk.getRecords().count());
      chunk.entry.setDensity(Entry.findDensityOfList(avgFileSize, intendedTransfer.getBandwidth()));
      chunk.entry.calculateSpecVector();
      LOG.info("Chunk " + i + ":\tfiles:" + partitions.get(i).getRecords().count() + "\t avg:" +
              Utils.printSize(partitions.get(i).getCentroid(), true)
              + "\t" + Utils.printSize(partitions.get(i).getRecords().size(), true) + " Density:" +
              chunk.entry.getDensity());
    }
    /*
    for (int i = 0; i < partitions.size(); i++) {
			XferList newList = partitions.get(i).getRecords().sliceLargeFiles(ConfigurationParams.MAXIMUM_SINGLE_FILE_SIZE);
			 partitions.get(i).setXferList(newList);
		}

		for (int i = 0; i < partitions.size(); i++) {
			Partition chunk = partitions.get(i);
			LOG.info("Chunk "+i+":\tfiles:"+partitions.get(i).getRecords().count()+"\t avg:"+
					Utils.printSize(partitions.get(i).getCentroid(), true)
					+"\t"+Utils.printSize(partitions.get(i).getRecords().size(), true)+" Density:" +
					chunk.entry.getDensity());
		}
		*/

    //System.exit(-1);
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
      } else {
        double[] chunkSize = new double[chunks.size()];
        for (int i = 0; i < chunks.size(); i++) {
          chunkSize[i] = chunks.get(i).getTotalSize();
          Density densityOfChunk = Entry.findDensityOfList(chunks.get(i).getCentroid(), intendedTransfer.getBandwidth());
          switch (densityOfChunk) {
            case SMALL:
              chunkWeights[i] = 6 * chunkSize[i];
              break;
            case MIDDLE:
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
        double avgFileSize = chunk.getRecords().size() / (chunk.getRecords().count() * 1.0);
        Density density = didclab.cse.buffalo.hysterisis.Entry.findDensityOfList(avgFileSize,
                CooperativeChannels.intendedTransfer.getBDP());
        LOG.info("Chunk " + density + " weight " + chunkWeights[i] + " cc: " + concurrencyLevels[i]);
      }
    }
    return concurrencyLevels;
  }

  private void parseArguments(String[] arguments) {
    String configFile  = arguments.length > 0 ? arguments[0] : "config.cfg";
    boolean noProxy = false;
    try (BufferedReader br = new BufferedReader(new FileReader(configFile))) {
      String line;
      while ((line = br.readLine()) != null) {
        String[] args = line.split("\\s+");
        String config = args[0];
        switch (config) {
          case "-s":
          case "-source":
            if (args.length > 1) {
              intendedTransfer.setSource(args[1]);
            } else {
              LOG.fatal("-source requires source address");
            }
            LOG.info("source  = " + intendedTransfer.getSource());
            break;
          case "-d":
          case "-destination":
            if (args.length > 1) {
              intendedTransfer.setDestination(args[1]);
            } else {
              LOG.fatal("-destination requires a destination address");
            }
            LOG.info("destination = " + intendedTransfer.getDestination());
            break;
          case "-proxy":
            if (args.length > 1) {
              proxyFile = args[1];
            } else {
              LOG.fatal("-path requires path of file/directory to be transferred");
            }
            LOG.info("proxyFile = " + proxyFile);
            break;
          case "-no-proxy":
            noProxy = true;
            break;
          case "-bw":
          case "-bandwidth":
            if (args.length > 1 || Double.parseDouble(args[1]) > 100) {
              intendedTransfer.setBandwidth(Math.pow(10, 9) * Double.parseDouble(args[1]));
            } else {
              LOG.fatal("-bw requires bandwidth in GB");
            }
            LOG.info("bandwidth = " + intendedTransfer.getBandwidth() + " GB");
            break;
          case "-rtt":
            if (args.length > 1) {
              intendedTransfer.setRtt(Double.parseDouble(args[1]));
            } else {
              LOG.fatal("-rtt requires round trip time in millisecond");
            }
            LOG.info("rtt = " + intendedTransfer.getRtt() + " ms");
            break;
          case "-maxcc":
          case "-max-concurrency":
            if (args.length > 1) {
              intendedTransfer.setMaxConcurrency(Integer.parseInt(args[1]));
            } else {
              LOG.fatal("-cc needs integer");
            }
            LOG.info("cc = " + intendedTransfer.getMaxConcurrency());
            break;
          case "-bs":
          case "-buffer-size":
            if (args.length > 1) {
              intendedTransfer.setBufferSize(Integer.parseInt(args[1]) * 1024 * 1024); //in MB
            } else {
              LOG.fatal("-bs needs integer");
            }
            LOG.info("bs = " + intendedTransfer.getBufferSize());
            break;
          case "-testbed":
            if (args.length > 1) {
              intendedTransfer.setTestbed(args[1]);
            } else {
              LOG.fatal("-testbed needs testbed name");
            }
            LOG.info("Testbed name is = " + intendedTransfer.getTestbed());
            break;
          case "-matlab-exec":
            if (args.length > 1) {
              ConfigurationParams.MATLAB_DIR = args[1];
            } else {
              LOG.fatal("-matlab installation path requires a full path name");
            }
            LOG.info("Matlab installation directory is = " + ConfigurationParams.MATLAB_DIR);
            break;
          case "-input":
            if (args.length > 1) {
              ConfigurationParams.INPUT_DIR = args[1];
            } else {
              LOG.fatal("-historical data input file path has to be passed");
            }
            LOG.info("Historical data path = " + ConfigurationParams.INPUT_DIR);
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
          default:
            System.err.println("Unrecognized input parameter " + config);
            System.exit(-1);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    if (null == proxyFile && !noProxy) {
      int uid = findUserId();
      proxyFile = "/tmp/x509up_u" + uid;
    }
    ConfigurationParams.init();
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

  enum TransferAlgorithm {SINGLECHUNK, MULTICHUNK}

  public enum Density {SMALL, MIDDLE, LARGE, HUGE}

  private enum ChannelDistributionPolicy {ROUND_ROBIN, WEIGHTED}


}
