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
	
	public CooperativeChannels() {
		// TODO Auto-generated constructor stub
		//initialize output streams for message logging
		LogManager.createLogFile(ConfigurationParams.STDOUT_ID);
		LogManager.createLogFile(ConfigurationParams.INFO_LOG_ID);
		intendedTransfer = new Entry();
		ConfigurationParams.init();
	}
	
	@VisibleForTesting
	public CooperativeChannels(GridFTPTransfer gridFTPClient) {
		this.gridFTPClient = gridFTPClient;
		//LogManager.createLogFile(ConfigurationParams.STDOUT_ID);
		//LogManager.createLogFile(ConfigurationParams.INFO_LOG_ID);
	}
	@VisibleForTesting
	public void setUseHysterisi(boolean bool){
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
		LOG.info("*************" + algorithm.name() + "************");
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
		
		//Get metadata information of dataset
		XferList dataset = gridFTPClient.getListofFiles(su.getPath(),du.getPath());
		LOG.info("mlsr completed at:"+((System.currentTimeMillis()-startTime)/1000.0));

		ArrayList<Partition> chunks = partitionByFileSize(dataset);
		int [][] estimatedParamsForChunks = new int[chunks.size()][4];
		if(useHysterisis) {
			hysterisis = new  Hysterisis(gridFTPClient);
			estimatedParamsForChunks = hysterisis.findOptimalParameters(chunks, intendedTransfer, dataset);
		}
		switch (algorithm){
			case SINGLECHUNK:
				if(useHysterisis) {
					for(int i = 0; i< chunks.size(); i++){
						estimatedParamsForChunks [i] = Utils.getBestParams(chunks.get(i).getRecords());
					}
				}
				for(int i = 0; i< chunks.size(); i++){
					int [] parameters = estimatedParamsForChunks[i];
					gridFTPClient.runTransfer(parameters[2], parameters[1], 
			    			parameters[0], parameters[3], chunks.get(i).getRecords(), i);
				}
				break;
			default:
				int totalChannelCount = intendedTransfer.getMaxConcurrency();
				if(useHysterisis) {
					int maxConcurrency = 0;
					for (int i = 0; i < estimatedParamsForChunks.length; i++){
						chunks.get(i).getRecords().params = estimatedParamsForChunks[i];
						if(estimatedParamsForChunks[i][0] > maxConcurrency)
							maxConcurrency = estimatedParamsForChunks[i][0];
					}
					LOG.info(" Running MC with :" +maxConcurrency + " channels.");
					totalChannelCount = maxConcurrency;
				}
				int []channelAllocation = allocateChannelsToChunks(chunks, totalChannelCount);
				gridFTPClient.runMultiChunkTransfer(chunks, channelAllocation);
				break;
		}
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
		for (int i = 0; i < 3; i++) {
			Partition p = new Partition();
			partitions.add(p);
		}
		for (XferList.Entry e : list) {
			if(e.size < intendedTransfer.getBDP()/2)
				partitions.get(0).addRecord(e);
			//else if(e.size < intendedTransfer.getBDP())
			//	partitions.get(1).addRecord(e);
			else if (e.size < intendedTransfer.getBDP()*10)
				partitions.get(1).addRecord(e);
			else
				partitions.get(2).addRecord(e);
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
			chunk.entry.setDensity( Entry.findDensityOfList(avgFileSize, intendedTransfer.getBDP()) );
			chunk.entry.calculateSpecVector();
			LOG.info("Chunk "+i+":\tfiles:"+partitions.get(i).getRecords().count()+"\t avg"+
					Utils.printSize(partitions.get(i).getRecords().size()/partitions.get(i).getRecords().count(), true)
					+"\t"+Utils.printSize(partitions.get(i).getRecords().size(), true)+" Density:" +
					chunk.entry.getDensity());
		}
		return partitions;
	}
	
	private int[] allocateChannelsToChunks(List<Partition> chunks, int channelCount){
		int totalChunks = chunks.size();
		int[] concurrencyLevels = new int[totalChunks];
		if(channelDistPolicy == ChannelDistributionPolicy.ROUND_ROBIN){
			List<Partition> reordered_chunks =Lists.newArrayListWithCapacity(totalChunks);
			for (int i = 0; i < (totalChunks + 1) /2  ; i++){
				reordered_chunks.add(chunks.get(i));
				if (i < totalChunks - i  - 1 )
					reordered_chunks.add(chunks.get(totalChunks - i - 1));
			}
			chunks = reordered_chunks;
			// Find channel distribution to chunks. Revised round-robin
			// e.g if cc= 6 and there're 4 chunks then, iterate as: 0-3-1-2-0-3
			// thus channel assignments becomes: 2-1-1-2
			for (int i = 0; i < channelCount; i++) {
				int chunkId = i % totalChunks;
				concurrencyLevels[chunkId]++;
			}
		}
		else {
			double[] estimatedThroughputs = hysterisis.getEstimatedThroughputs();
			double totalThroughput = 0;
			for (int i = 0; i < estimatedThroughputs.length; i++) {
				totalThroughput += estimatedThroughputs[i];
			}
			double totalWeight = 0;
			double[] chunkWeights = new double[chunks.size()];
			for (int i = 0; i < estimatedThroughputs.length; i++) {
				chunkWeights[i] = chunks.get(i).getTotalSize() * (totalThroughput / estimatedThroughputs[i]);
				totalWeight += chunkWeights[i];
			}
			int assignedChannelCount = 0;
			for (int i = 0; i < chunks.size(); i++) {
				double propChunkWeight = (chunkWeights[i]*1.0/totalWeight);
				concurrencyLevels[i] = (int) Math.floor(channelCount * propChunkWeight);
				assignedChannelCount += concurrencyLevels[i];
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
				LOG.info("Chunk "+ density + " EstimatedThr:"+ estimatedThroughputs[i] + " weight "+chunkWeights[i] + " cc: "+ concurrencyLevels[i]);
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
			else if (arg.equals("-useHysterisis")) {
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
				algorithm = TransferAlgorithm.SINGLECHUNK;
				LOG.info("Use single chunk transfer approach");
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
		else
			LOG.info("Success!");
	}



}
