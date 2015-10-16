package didclab.cse.buffalo;


import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import didclab.cse.buffalo.hysterisis.Entry;
import didclab.cse.buffalo.hysterisis.Hysterisis;
import didclab.cse.buffalo.log.LogManager;
import didclab.cse.buffalo.utils.Utils;
import stork.module.CooperativeModule.GridFTPTransfer;
import stork.util.XferList;


public class CooperativeChannels {

	private static final Log LOG = LogFactory.getLog(CooperativeChannels.class);
	public enum TransferAlgorithm{SINGLECHUNK, MULTICHUNK};
	public enum Density{SMALL, MIDDLE, LARGE, HUGE};
	public static Entry intendedTransfer;

	static String proxyFile;
	private GridFTPTransfer gridFTPClient;
	static double totalTransferTime = 0;
	private boolean useHysterisis = false;
	public enum ChannelDistributionPolicy{ROUND_ROBIN, WEIGHTED};
	ChannelDistributionPolicy channelDistPolicy = ChannelDistributionPolicy.WEIGHTED;

	public TransferAlgorithm algorithm = TransferAlgorithm.MULTICHUNK;
	Hysterisis hysterisis;
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
	@VisibleForTesting
	public void setUseHysterisis(boolean bool){
		useHysterisis = bool;
	}

	public static void main(String[] args) throws Exception {
		CooperativeChannels multiChunk = new CooperativeChannels();
		multiChunk.parseArguments(args);
		multiChunk.transfer();
	}
	@VisibleForTesting
	void transfer() throws Exception{
		intendedTransfer.setBDP( (intendedTransfer.getBandwidth() * intendedTransfer.getRtt())/8 ); // In MB
		String mHysterisis = useHysterisis ? "Hysterisis" : "";
		String mDynamic = useDynamicScheduling ? "Dyanmic" : "";
		LOG.info("*************" + algorithm.name() + "************");
		LogManager.writeToLog("*************" + algorithm.name()+ "-" + mHysterisis+ "-" + mDynamic + "************" , ConfigurationParams.INFO_LOG_ID);
		URI su = null ,du = null;
		try {
			su = new URI(intendedTransfer.getSource()).normalize();
			du = new URI(intendedTransfer.getDestination()).normalize();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}

		// create Control Channel to source and destination server
		double startTime = System.currentTimeMillis();
		if(gridFTPClient == null) {
			gridFTPClient = new GridFTPTransfer(proxyFile,su,du);
			gridFTPClient.start();
			gridFTPClient.waitFor();
		}
		if(gridFTPClient == null || gridFTPClient.client ==  null) {
			LOG.info("GridFTP client connection couldnot be established. Exiting...");
			System.exit(-1);
		}
		gridFTPClient.useDynamicScheduling = useDynamicScheduling;
		if(useHysterisis) {
			// this will initialize matlab connection while running hysterisis analysis
			hysterisis = new  Hysterisis(gridFTPClient);
		}
		
		//Get metadata information of dataset
		XferList dataset = gridFTPClient.getListofFiles(su.getPath(),du.getPath());
		LOG.info("mlsr completed at:"+((System.currentTimeMillis()-startTime)/1000.0) + "set size:" + dataset.size());

		long datasetSize = dataset.size();
		ArrayList<Partition> chunks = partitionByFileSize(dataset);

		int [][] estimatedParamsForChunks = new int[chunks.size()][4];
		long timeSpent = 0;
		if(useHysterisis) {
			estimatedParamsForChunks = hysterisis.findOptimalParameters(chunks, intendedTransfer, dataset);
			for(int i = 0; i< chunks.size(); i++){
				timeSpent += chunks.get(i).getSamplingTime();
			}
			timeSpent += hysterisis.optimizationAlgorithmTime;
			//estimatedParamsForChunks[0][2] = 17;
			gridFTPClient.client.chunks.clear();
		}
		switch (algorithm){
			case SINGLECHUNK:
				if(!useHysterisis) {
					for(int i = 0; i< chunks.size(); i++){
						estimatedParamsForChunks [i] = Utils.getBestParams(chunks.get(i).getRecords());
					}
				}
				for(int i = 0; i< chunks.size(); i++){
					int [] parameters = estimatedParamsForChunks[i];
					long start = System.currentTimeMillis();
					gridFTPClient.runTransfer(parameters[0], parameters[1], 
			    			parameters[2], parameters[3], chunks.get(i).getRecords(), i);
					timeSpent += ((System.currentTimeMillis() - start )/1000.0);
				}
				break;
			default:
				int totalChannelCount = intendedTransfer.getMaxConcurrency();
				if(useHysterisis) {
					int maxConcurrency = 0;
					for (int i = 0; i < estimatedParamsForChunks.length; i++){
						chunks.get(i).getRecords().setTransferParameters(estimatedParamsForChunks[i]);
						if(estimatedParamsForChunks[i][0] > maxConcurrency)
							maxConcurrency = estimatedParamsForChunks[i][0];
					}
					LOG.info(" Running MC with :" +maxConcurrency + " channels.");
					totalChannelCount = maxConcurrency;
				}
				else {
					for (int i = 0; i < estimatedParamsForChunks.length; i++) {
						chunks.get(i).getRecords().setTransferParameters(Utils.getBestParams(chunks.get(i).getRecords()));
					}
				}
				int []channelAllocation = allocateChannelsToChunks(chunks, totalChannelCount);
				long start = System.currentTimeMillis();
				gridFTPClient.runMultiChunkTransfer(chunks, channelAllocation);
				timeSpent += (System.currentTimeMillis() - start )/1000.0;
				break;
		}
		LogManager.writeToLog("Final throughput:" + (datasetSize*8.0)/(timeSpent*(1000.0*1000)), ConfigurationParams.INFO_LOG_ID);
		gridFTPClient.executor.shutdown();
        while (!gridFTPClient.executor.isTerminated()) {
        }
		LogManager.close();
		gridFTPClient.stop();
	}

	double totalChunksSize(Partition p ){
		double sum = 0;
		for (int i = 0; i < p.getRecords().count(); i++) {
			sum += p.getRecords().getItem(i).size;
		}
		return sum;
	}

	ArrayList<Partition> mergePartitions(ArrayList<Partition> partitions){
		for (int i = 0; i < partitions.size(); i++) {
			Partition p  =  partitions.get(i);
			if(p.getRecords().count() <= 2 || p.getRecords().size() < 5 * intendedTransfer.getBDP()){  //merge small chunk with the the chunk with closest centroid
				int index = -1;
				double diff = Double.POSITIVE_INFINITY;
				for (int j = 0; j < partitions.size(); j++) {
					if(j != i && Math.abs(p.getCentroid()-partitions.get(j).getCentroid()) < diff){
						diff = Math.abs(p.getCentroid()-partitions.get(j).getCentroid());
						index = j;
					}
				}
				if(index == -1){
					LOG.fatal("Fatal error: Could not find chunk to merge!");
					System.exit(-1);
				}
				partitions.get(index).getRecords().addAll(p.getRecords());
				LOG.info("Partition "+i+" "+p.getRecords().count() +" files "+ Utils.printSize(p.getRecords().size(), true));
				LOG.info("Merging partition "+i+" to partition "+index);
				partitions.remove(i);
				i--;
			}
		}
		return partitions;
	}

	ArrayList<Partition> partitionByFileSize(XferList list){

		//remove folders from the list and send mkdir command to destination
		for (int i = 0; i < list.count(); i++) {
			if(list.getItem(i).dir){
				list.removeItem(i);
			}
		}

		ArrayList<Partition> partitions=  new ArrayList<Partition>();
		for (int i = 0; i < 4; i++) {
			Partition p = new Partition();
			partitions.add(p);
		}
		for (XferList.Entry e : list) {
			 double bandwidthInBytes = intendedTransfer.getBandwidth() / 8;
             if(e.size < bandwidthInBytes/20)
                     partitions.get(0).addRecord(e);
             else if(e.size < bandwidthInBytes/5)
                     partitions.get(1).addRecord(e);
             else if (e.size < bandwidthInBytes*2)
                     partitions.get(2).addRecord(e);
             else
                     partitions.get(3).addRecord(e);
		}
		mergePartitions(partitions);
		for (int i = 0; i < partitions.size(); i++) {
			Partition chunk = partitions.get(i);
			chunk.getRecords().sp = list.sp;
			chunk.getRecords().dp = list.dp;
			double avgFileSize = chunk.getRecords().size()/(chunk.getRecords().count()*1.0);
			chunk.setEntry(intendedTransfer);
			chunk.entry.setFileSize( avgFileSize );
			chunk.entry.setFileCount( chunk.getRecords().count());
			chunk.entry.setDensity( Entry.findDensityOfList(avgFileSize, intendedTransfer.getBandwidth()) );
			chunk.entry.calculateSpecVector();
			LOG.info("Chunk "+i+":\tfiles:"+partitions.get(i).getRecords().count()+"\t avg:"+
					Utils.printSize(partitions.get(i).getCentroid(), true)
					+"\t"+Utils.printSize(partitions.get(i).getRecords().size(), true)+" Density:" +
					chunk.entry.getDensity());
		}
		return partitions;
	}
	
	private int[] allocateChannelsToChunks(List<Partition> chunks, final int channelCount){
		int totalChunks = chunks.size();
		int[] concurrencyLevels = new int[totalChunks];
		if(channelDistPolicy == ChannelDistributionPolicy.ROUND_ROBIN){
			int modulo = (totalChunks + 1) /2 ;
			int count = 0;
			for (int i = 0; count < channelCount ; i++){
				int index = i % modulo ;
				concurrencyLevels[index]++;
				count++;
				if (index < totalChunks - index  - 1 && count < channelCount) {
					concurrencyLevels[totalChunks - index - 1]++;
					count++;
				}
			}
			
			for (int i = 0; i < totalChunks; i++) {
				System.out.println("Chunk "+i + ":"+ concurrencyLevels[i] +"channels");
			}
		}
		else {
			double[] chunkWeights = new double[chunks.size()];
			double totalWeight = 0;
			if(useHysterisis){
				int [][]estimatedParams = hysterisis.getEstimatedParams();
				double[] estimatedThroughputs = hysterisis.getEstimatedThroughputs();
				double[] estimatedUnitThroughputs = new double[chunks.size()];
				for (int i = 0; i < chunks.size(); i++) {
					estimatedUnitThroughputs[i] = estimatedThroughputs[i] / estimatedParams[i][0];
					LOG.info("estimated unit thr:" + estimatedUnitThroughputs[i]  + " " + estimatedParams[i][0]);
				}
				double totalThroughput = 0;
				for (int i = 0; i < estimatedUnitThroughputs.length; i++) {
					totalThroughput += estimatedUnitThroughputs[i];
				}
				for (int i = 0; i < estimatedThroughputs.length; i++) {
					chunkWeights[i] = chunks.get(i).getTotalSize() * (totalThroughput / estimatedUnitThroughputs[i]);
					totalWeight += chunkWeights[i];
				}
			}
			else {
				double[] chunkSize = new double[chunks.size()];
				for (int i = 0; i < chunks.size(); i++) {
					chunkSize[i] = chunks.get(i).getTotalSize();
					Density densityOfChunk = Entry.findDensityOfList(chunks.get(i).getCentroid(), intendedTransfer.getBandwidth());
					switch (densityOfChunk){
						case SMALL:
							chunkWeights[i] = 6*chunkSize[i] ;
							break;
						case MIDDLE:
							chunkWeights[i] = 3*chunkSize[i] ;
							break;
						case LARGE:
							chunkWeights[i] = 2*chunkSize[i] ;
							break;
						case HUGE:
							chunkWeights[i] = 1*chunkSize[i] ;
							break;
						default:
							break;
					}
					totalWeight +=chunkWeights[i];
				}
			}
			
			int assignedChannelCount = 0;
			for (int i = 0; i < (totalChunks + 1 /2) && assignedChannelCount < channelCount; i++) {
				double propChunkWeight = (chunkWeights[i]*1.0/totalWeight);
				int remaining_channels = channelCount - assignedChannelCount;
				concurrencyLevels[i] = Math.min(remaining_channels, (int) Math.floor(channelCount * propChunkWeight));
				assignedChannelCount += concurrencyLevels[i];
				if(i < totalChunks - i - 1 && assignedChannelCount < channelCount){
					propChunkWeight = (chunkWeights[totalChunks - i - 1]*1.0/totalWeight);
					remaining_channels = channelCount - assignedChannelCount;
					concurrencyLevels[totalChunks - i - 1] = Math.min(remaining_channels, (int) Math.floor(channelCount * propChunkWeight));
					assignedChannelCount += concurrencyLevels[totalChunks - i - 1];
				}
			}
			
			// Since we take floor when calculating, total channels might be unassigned.
			// If so, starting from chunks with zero channels, assign remaining channels
			// in round robin fashion
			while(assignedChannelCount < channelCount){
				for (int i = 0; i < chunks.size(); i++) {
					if(concurrencyLevels[i] == 0 && assignedChannelCount < channelCount) {
						concurrencyLevels[i]++;
						assignedChannelCount++;
						i = 0;
					}
				}
				if(assignedChannelCount >= channelCount)
					break;
				//find the chunks with minimum assignedChannelCount
				int minChannelCount = Integer.MAX_VALUE;
				int chunkIdWithMinChannel = -1;
				for (int i = 0; i < chunks.size(); i++) {
					if(concurrencyLevels[i] < minChannelCount) {
						minChannelCount = concurrencyLevels[i];
						chunkIdWithMinChannel = i;
					}
				}
				concurrencyLevels[chunkIdWithMinChannel]++;
				assignedChannelCount++;
			}
			for (int i = 0; i < totalChunks; i++) {
				Partition chunk = chunks.get(i);
				double avgFileSize = chunk.getRecords().size()/(chunk.getRecords().count()*1.0);
				Density density = didclab.cse.buffalo.hysterisis.Entry.findDensityOfList(avgFileSize,
						CooperativeChannels.intendedTransfer.getBDP());
				LOG.info("Chunk "+ density + " weight "+chunkWeights[i] + " cc: "+ concurrencyLevels[i]);
			}
		}		
		return concurrencyLevels;
	}

	void parseArguments(String[] args){
		int i = 0;
		String arg;
		while (i < args.length && args[i].startsWith("-")) {
			arg = args[i++];
			// use this type of check for arguments that require arguments
			if (arg.equals("-s") || arg.equals("-source")) {
				if (i < args.length)
					intendedTransfer.setSource(args[i++]);
				else
					LOG.fatal("-source requires source address");
				LOG.info("source  = " + intendedTransfer.getSource());
			}

			else if (arg.equals("-d") || arg.equals("-destination")) {
				if (i < args.length)
					intendedTransfer.setDestination(args[i++]);
				else
					LOG.fatal("-destination requires a destination address");
				LOG.info("destination = " + intendedTransfer.getDestination());
			}
			else if (arg.equals("-proxy")) {
				if (i < args.length)
					proxyFile = args[i++];
				else
					LOG.fatal("-path requires path of file/directory to be transferred");
				LOG.info("proxyFile = " + proxyFile);
			}

			else if (arg.equals("-bw")) {
				if (i < args.length)
					intendedTransfer.setBandwidth(Math.pow(10, 9)  * Double.parseDouble(args[i++]) );
				else
					LOG.fatal("-bw requires bandwidth in GB");
				LOG.info("bandwidth = " + intendedTransfer.getBandwidth()+" GB");
			}
			else if (arg.equals("-rtt")) {
				if (i < args.length)
					intendedTransfer.setRtt( Double.parseDouble(args[i++]) );
				else
					LOG.fatal("-rtt requires round trip time in millisecond");
				LOG.info("rtt = " + intendedTransfer.getRtt()+" ms");
			}
			else if (arg.equals("-cc")) {
				if (i < args.length)
					intendedTransfer.setMaxConcurrency( Integer.parseInt(args[i++]) );
				else
					LOG.fatal("-cc needs integer");
				LOG.info("cc = " + intendedTransfer.getMaxConcurrency());
			}
			else if (arg.equals("-bs")) {
				if (i < args.length)
					intendedTransfer.setBufferSize(Integer.parseInt(args[i++]) * 1024 *1024); //in MB
				else
					LOG.fatal("-bs needs integer");
				LOG.info("bs = " + intendedTransfer.getBufferSize());
			}
			else if (arg.equals("-testbed")) {
				if (i < args.length)
					intendedTransfer.setTestbed( args[i++] );
				else
					LOG.fatal("-testbed needs testbed name");
				LOG.info("Testbed name is = " + intendedTransfer.getTestbed());
			}
			else if (arg.equals("-matlab")) {
				if (i < args.length)
					ConfigurationParams.MATLAB_DIR = args[i++];
				else
					LOG.fatal("-matlab installation path requires a full path name");
				LOG.info("Matlab installation directory is = " + ConfigurationParams.MATLAB_DIR);
			}
			else if (arg.equals("-home")) {
				if (i < args.length)
					ConfigurationParams.HOME_DIR = args[i++];
				else
					LOG.fatal("-matlab installation path requires a full path name");
				LOG.info("Project directory = " + ConfigurationParams.HOME_DIR);
			}
			else if (arg.equals("-input")) {
				if (i < args.length)
					ConfigurationParams.INPUT_DIR = args[i++];
				else
					LOG.fatal("-historical data input file path has to be passed");
				LOG.info("Historical data path = " + ConfigurationParams.INPUT_DIR);
			}
			else if (arg.equals("-use-hysterisis")) {
				useHysterisis = true;
				LOG.info("Use hysterisis based approach");
			}
			else if (arg.equals("-single-chunk")) {
				algorithm = TransferAlgorithm.SINGLECHUNK;
				LOG.info("Use single chunk transfer approach");
			}
			else if (arg.equals("-channel-distribution-policy")) {
				if (i < args.length) {
					if(args[i++].compareTo("roundrobin") == 0)
						channelDistPolicy = ChannelDistributionPolicy.ROUND_ROBIN;
					else if(args[i++].compareTo("weighted") == 0)
						channelDistPolicy = ChannelDistributionPolicy.WEIGHTED;
					else
						LOG.fatal("-channel-distribution-policy can be either \"roundrobin\" or \"weighted\"");
				}
				else
					LOG.fatal("-channel-distribution-policy has to be specified as \"roundrobin\" or \"weighted\"");
			}
			else if (arg.equals("-use-dynamic-scheduling")) {
				useDynamicScheduling = true;
				LOG.info("Dynamic scheduling enabled.");
			}
			else {
				System.err.println("Unrecognized input parameter "+arg);
				System.exit(-1);
			}
		}
		if (i != args.length){
			LOG.fatal("Usage: ParseCmdLine [-verbose] [-xn] [-output afile] filename");
			LOG.info(args[i]);
			System.exit(0);
		}
		else {
			LOG.info("Success!");
			ConfigurationParams.init();
		}
	}



}
