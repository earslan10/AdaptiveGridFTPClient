package didclab.cse.buffalo;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.util.ArrayList;

import didclab.cse.buffalo.hysterisis.Hysterisis;
import didclab.cse.buffalo.hysterisis.Entry;
import didclab.cse.buffalo.log.LogManager;
import stork.module.CooperativeModule.GridFTPTransfer;
import stork.util.XferList;



public class CooperativeChannels {

	public enum Density{SMALL, MIDDLE, LARGE, HUGE};
	public static Entry targetTransfer;

	
	public static void main(String[] args) throws Exception {
		
		parseArguments(args);
		ConfigurationParams.BDP =  (ConfigurationParams.BANDWIDTH* ConfigurationParams.RTT*0.001)/8; // In MB
		
		URI su =null ,du=null;
		try {
			su = new URI(ConfigurationParams.SourceServer).normalize();
			du = new URI(ConfigurationParams.DestinationServer).normalize();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// create Control Channel to source and destination server
		double startTime = System.currentTimeMillis();
		ConfigurationParams.GridFTPClient = new GridFTPTransfer(ConfigurationParams.ProxyFile,su,du);
		ConfigurationParams.GridFTPClient.start();
		ConfigurationParams.GridFTPClient.waitFor();
		
		//Get metadata information of dataset
		XferList xList = ConfigurationParams.GridFTPClient.client.getListofFiles(su.getPath(),du.getPath());
		LogManager.writeToLog("mlsr completed at:"+((System.currentTimeMillis()-startTime)/1000.0),ConfigurationParams.STDOUT_ID);
		
		
		
		//remove folders from the list and send mkdir command to destination
		for (int i = 0; i < xList.count(); i++) {
			if(xList.getItem(i).dir){
				xList.removeItem(i);
			}
		}
		
		
		
		//initialize output streams for message logging
		LogManager.createLogFile(ConfigurationParams.STDOUT_ID);
		LogManager.createLogFile(ConfigurationParams.INFO_LOG_ID);
		
		
		LogManager.writeToLog(ConfigurationParams.STDOUT_ID, "**********************Cooperative Chunks cc="+ConfigurationParams.MAX_CONCURRENCY+"**************");
		LogManager.writeToLog(ConfigurationParams.INFO_LOG_ID,"******************************Cooperative Chunks cc="+ConfigurationParams.MAX_CONCURRENCY+"***************\n");
		
		ArrayList<Partition> chunks = partitionByFileSize(xList);
		//double totalDataSize = 0;
		for (Partition chunk : chunks) {
			//totalDataSize+= chunk.getRecords().size();
			chunk.getRecords().sp = xList.sp;
			chunk.getRecords().dp = xList.dp;
			double avgFileSize = chunk.getRecords().size()/(chunk.getRecords().count()*1.0);
			CooperativeChannels.targetTransfer.setFileSize( avgFileSize );
			CooperativeChannels.targetTransfer.setFileCount( chunk.getRecords().count() );
			CooperativeChannels.targetTransfer.setDensity( Entry.findDensityOfList(avgFileSize, ConfigurationParams.BDP) );
			CooperativeChannels.targetTransfer.calculateSpecVector();
		}
		
		if(ConfigurationParams.USE_HISTORY){
			
			Hysterisis hysterisis = new Hysterisis();
			hysterisis.parseInputFiles();
			if(hysterisis.getInputFiles().size() > 0){
				double []sampleThroughputs = new double[chunks.size()];
				//Run sample transfer to learn current network load
		    	{
		    		/*
		    		 * //Create sample dataset to and transfer to measure current load on the network
		    		 */
			    	for (int chunkNumber = 0 ; chunkNumber < chunks.size() ; chunkNumber++) {
			    		Partition chunk =  chunks.get(chunkNumber);
			    		
			        	XferList sample_files = new XferList("", "") ;
			        	double MINIMUM_SAMPLING_SIZE = 10 * ConfigurationParams.BDP;
			        	while (sample_files.size() < MINIMUM_SAMPLING_SIZE || sample_files.count() < 2){ 
			        		XferList.Entry file = chunk.getRecords().pop();
			        		sample_files.add(file.path, file.size);
			        	}
			        	
			        	sample_files.sp = xList.sp;
			        	sample_files.dp = xList.dp;
					    sampleThroughputs[chunkNumber] = runSampleDataTransfer(sample_files);
					    //If transfer failed: To be implemented 
					    if(sampleThroughputs[chunkNumber] == -1)System.exit(-1);	
			    	}
		    	}
		    	
		    	hysterisis.runMatlabModeling(chunks, sampleThroughputs);
			}
			
			
			
	    	
	    	
	    	
	    	/*double [] parameters = (double []) results[0];
			double throuhput = ((double []) results[1]) [0];
			System.out.println("Results:"+ throuhput +" "+Math.round(parameters[0])+" "+parameters[1]+" "+parameters[2]);
			out.write("\tselected parameters: cc:" +Math.round(parameters[0])+" p:"+Math.round(parameters[1])
						+" ppq:"+Math.round(parameters[2])+"\n");
			out.flush();
		    double chunkSize = chunk.getRecords().size();
			chunk.getRecords().sp = xList.sp;
			chunk.getRecords().dp = xList.dp;
			
			long start = System.currentTimeMillis();
			
			//
	    	int []params = new int[4];
	    	params[2] = (int) Math.min(Math.min(MAX_CONCURRENCY, Math.round(parameters[0])), chunk.getRecords().count() );
	    	params[1] =(int) Math.round(parameters[1]);
	    	params[0] = (int) Math.min(Math.round(parameters[2]), chunk.getRecords().count()-params[2]);
	    	params[3] = bufferSize;
	    	
	    	out.append("Sampling files:"+chunk.getRecords().count()+"  Centroid:"+ 
	    		printSize(chunk.getCentroid())+" total:"+printSize(chunk.getRecords().size())+"\tppq:"+params[0]+"*p:"+params[1]
	    		+"*"+"*cc:"+params[2]+"\n");
	    	out.flush();
			System.out.println("Sampling files:"+chunk.getRecords().count()+"  Centroid:"+printSize(chunk.getCentroid())+" total:"+printSize(chunk.getRecords().size())+"\tppq:"+params[0]+"*p:"+params[1]+"*"+"*cc:"+params[2]); 
			
			tf.startTransfer(params[0], params[1], params[2], params[3], chunk.getRecords());
			
			double timeSpent = (System.currentTimeMillis()-start)/1000.0;
			totalTime+=timeSpent;
			double thr = chunkSize/timeSpent;
			out.append("Time spent:"+ timeSpent+" chunk size:"+printSize(chunkSize)+" Throughput:"+ printSize(thr)+"\n");
			System.out.println("Time spent:"+ timeSpent+" chunk size:"+printSize(chunkSize)+" Throughput:"+ printSize(thr)+"\n*********");
			out.flush();
			chunkNumber++;*/
			
		}
		/*
		
		tf.transfer(chunks,maxChannels);

		
		long end = System.currentTimeMillis();
		double thr = totalDataSize/((end-init)/1000.0);
		System.out.println("Overall Time:"+((end-init)/1000.0)+" sec Thr:"+ printSize(thr));
		out.append(" Time:"+((end-init)/1000.0)+" sec Thr:"+ printSize(thr)+"/s\n");
		out.flush();
		out.close();
		tf.stop();
		
		/*
		Date endDate = new Date();
		FileWriter fstream2 = new FileWriter("startTimes.log",true);
		out = new BufferedWriter(fstream2);
		out.write("Cooperative Chunks cc="+maxChannels+" start:"+start+"\tend:"+endDate);
		out.flush();
		out.close();
		*/
		
		
	}
	
	/*
	 * Run sample data transfer to learn current network load
	 * sample_files is supposed to be set of files from highest density chunk
	 */
	public static double runSampleDataTransfer(XferList sample_files) {
		
		
    	//parallelism, pipelining and concurrency parameters 
    	int []params = getBestParams(sample_files);
    	
    	double measuredThroughput = -1;
    	try{
	    	//Run sampling transfer
	    	LogManager.writeToLog("Sampling chunk --> files:"+sample_files.count()+
	    			   " Centroid:"+printSize(sample_files.size()/sample_files.count())+
	    		 	   " total:"+printSize(sample_files.size())+"\tppq:"+params[2]+"*p:"+
	    			    params[1]+"*"+"*cc:"+params[0]+"\n", ConfigurationParams.INFO_LOG_ID);
	    	double fileSize = sample_files.size();
	    	long init = System.currentTimeMillis();
	    	ConfigurationParams.GridFTPClient.startTransfer(params[2], params[1], params[0], params[3], sample_files);
			
			double timeSpent = (System.currentTimeMillis()-init)/1000.0;
			//totalTransferTime += timeSpent;
			measuredThroughput = fileSize*8/timeSpent;
			LogManager.writeToLog("Time spent:"+ timeSpent+" chunk size:"+printSize(fileSize)+" Throughput:"+
								printSize(measuredThroughput)+"\n*********", ConfigurationParams.STDOUT_ID, ConfigurationParams.INFO_LOG_ID);
    	}
    	catch(Exception e){
    		e.printStackTrace();
    		System.exit(0);
    	}
		return measuredThroughput;
	}
	
	double totalChunksSize(Partition p ){
		double sum = 0;
		for (int i = 0; i < p.getRecords().count(); i++) {
			sum += p.getRecords().getItem(i).size;
		}
		return sum;
	}


	static ArrayList<Partition> mergePartitions(ArrayList<Partition> partitions){
		for (int i = 0; i < partitions.size(); i++) {
			Partition p  =  partitions.get(i);
			if(p.getRecords().count() <= 2 || p.getRecords().size() < 2*ConfigurationParams.BDP){  //merge small chunk with the the chunk with closest centroid
				int index = -1;
				double diff = Double.POSITIVE_INFINITY;
				for (int j = 0; j < partitions.size(); j++) {
					if(j!=i && Math.abs(p.getCentroid()-partitions.get(j).getCentroid()) < diff){
						diff = Math.abs(p.getCentroid()-partitions.get(j).getCentroid());
						index = j;
					}
				}
				if(index == -1){
					System.err.println("Fatal error: Could not find chunk to merge!");
					System.exit(0);
				}
				partitions.get(index).getRecords().addAll(p.getRecords());
				System.out.println("Partition "+i+" "+p.getRecords().count() +" files "+ printSize(p.getRecords().size()));
				System.out.println("Merging partition "+i+" to partition "+index);
				partitions.remove(i);
				i--;
			}
		}
		return partitions;
	}
	
	static ArrayList<Partition> partitionByFileSize(XferList list){
		ArrayList<Partition> partitions=  new ArrayList<Partition>();
		for (int i = 0; i < 4; i++) {
			Partition p = new Partition();
			partitions.add(p);
		}
		for (XferList.Entry e : list) {
			if(e.size < ConfigurationParams.BDP/10)
				partitions.get(0).addRecord(e);
			else if(e.size < ConfigurationParams.BDP/2)
				partitions.get(1).addRecord(e);
			else if (e.size < ConfigurationParams.BDP*10)
				partitions.get(2).addRecord(e);
			else
				partitions.get(3).addRecord(e);
		}
		mergePartitions(partitions);
		for (int i = 0; i < partitions.size(); i++) {
			System.out.println("Chunk "+i+":\tfiles:"+partitions.get(i).getRecords().count()+"\t avg"+printSize(partitions.get(i).getRecords().size()/partitions.get(i).getRecords().count())+"\t"+printSize(partitions.get(i).getRecords().size()));
		}
		return partitions;
	}
	



	public static String printSize(double random)
	{	
		DecimalFormat df = new DecimalFormat("###.##");
		if(random<1024.0)
			return df.format(random)+" B";
		else if(random<1024.0*1024)
			return df.format(random/1024.0)+" KB";
		else if(random<1024.0*1024*1024)
			return df.format(random/(1024.0*1024))+" MB";
		else if (random <(1024*1024*1024*1024.0))
			return df.format(random/(1024*1024.0*1024))+" GB";
		else 
			return df.format(random/(1024*1024*1024.0*1024))+" TB";
	}



	/*
	This function find what is average file size in a given directory
	 
	static Density findDensityOfList(XferList list){
		double average = list.size()/list.count();
		if(average < BDP/10)
			return Density.SMALL;
		else if(average < BDP/2)
			return Density.MIDDLE;
		else if (average < 20 * BDP)
			return Density.LARGE;
		return Density.HUGE;
	}	
	*/
	

	public static int[] getBestParams(XferList xl){
		Density density = Entry.findDensityOfList(xl.avgFileSize(), ConfigurationParams.BDP);
		xl.density = density;
		double avgFileSize = xl.avgFileSize();
		int fileCountToFillThePipe = (int)Math.round(ConfigurationParams.BDP/avgFileSize);
		int paralleStreamCountToFillPipe = (int)Math.round(ConfigurationParams.BDP/ConfigurationParams.BufferSize);
		int paralleStreamCountToFillBuffer = (int)Math.round(avgFileSize/ConfigurationParams.BufferSize);
		int cc = Math.max(Math.min(Math.min(fileCountToFillThePipe, xl.count()), ConfigurationParams.MAX_CONCURRENCY), 2);
		int ppq = fileCountToFillThePipe;
		int p =Math.max(Math.min(paralleStreamCountToFillPipe, paralleStreamCountToFillBuffer) , 1);
		return new int[] {cc,p,ppq,ConfigurationParams.BufferSize};
		/*
		if(density == Density.SMALL)
			return new int[] {ppq,p,cc,bufferSize};
		else if(density == Density.MIDDLE)
			return new int[] {ppq,Math.min(2,(int)(Math.ceil(BDP/bufferSize))+1), cc,bufferSize};
		else if(density == Density.LARGE)
			return new int[] {ppq,(int)(Math.ceil(BDP/bufferSize)+2), cc,bufferSize};
		else 
			return new int[] {ppq,(int)(Math.ceil(BDP/bufferSize)+2), cc,bufferSize};
		*/
	}
	static void parseArguments(String[] args){
		int i = 0;
		String arg;
		boolean vflag = true;
		while (i < args.length && args[i].startsWith("-")) {
			arg = args[i++];
			// use this type of check for "wordy" arguments
			if (arg.equals("-verbose")) {
				System.out.println("verbose mode on");
				vflag = true;
			}

			// use this type of check for arguments that require arguments
			else if (arg.equals("-s") || arg.equals("-source")) {
				if (i < args.length)
					targetTransfer.setSource(args[i++]);
				else
					System.err.println("-source requires source address");
				if (vflag)
					System.out.println("source  = " + ConfigurationParams.SourceServer);
			}

			else if (arg.equals("-d") || arg.equals("-destination")) {
				if (i < args.length)
					targetTransfer.setDestination(args[i++]);
				else
					System.err.println("-destination requires a destination address");
				if (vflag)
					System.out.println("destination = " + ConfigurationParams.DestinationServer);
			}
			else if (arg.equals("-proxy")) {
				if (i < args.length)
					ConfigurationParams.ProxyFile = args[i++];
				else
					System.err.println("-path requires path of file/directory to be transferred");
				if (vflag)
					System.out.println("proxyFile = " + ConfigurationParams.ProxyFile);
			}

			else if (arg.equals("-bw")) {
				if (i < args.length)
					targetTransfer.setBandwidth(Math.pow(10, 9)  * Double.parseDouble(args[i++]) );
				else
					System.err.println("-bw requires bandwidth in GB");
				if (vflag)
					System.out.println("bandwidth = " + ConfigurationParams.BANDWIDTH+" GB");
			}
			else if (arg.equals("-rtt")) {
				if (i < args.length)
					targetTransfer.setRtt( Double.parseDouble(args[i++]) );
				else
					System.err.println("-rtt requires round trip time in millisecond");
				if (vflag)
					System.out.println("rtt = " + ConfigurationParams.RTT+" ms");
			}
			else if (arg.equals("-cc")) {
				if (i < args.length)
					ConfigurationParams.MAX_CONCURRENCY = Integer.parseInt(args[i++]);
				else
					System.err.println("-cc needs integer");
				if (vflag)
					System.out.println("cc = " + ConfigurationParams.MAX_CONCURRENCY);
			}
			else if (arg.equals("-bs")) {
				if (i < args.length)
					targetTransfer.setBufferSize(Integer.parseInt(args[i++]) * 1024 *1024); //in MB
				else
					System.err.println("-bs needs integer");
				if (vflag)
					System.out.println("bs = " + ConfigurationParams.BufferSize);
			}
			else if (arg.equals("-testbed")) {
                if (i < args.length)
                        targetTransfer.setTestbed( args[i++] );
                else
                        System.err.println("-testbed needs testbed name");
                if (vflag)
                        System.out.println("Testbed name is = " + ConfigurationParams.TESTBED);
			}
			else if (arg.equals("-matlab-dir")) {
                if (i < args.length)
                        ConfigurationParams.MATLAB_DIR = args[i++];
                else
                        System.err.println("-matlab installation path requires a full path name");
                if (vflag)
                        System.out.println("Matlab installation directory is = " + ConfigurationParams.MATLAB_DIR);
			}
			else if (arg.equals("-input")) {
                if (i < args.length)
                        ConfigurationParams.INPUT_DIR = args[i++];
                else
                        System.err.println("-historical data input file path has to be passed");
                if (vflag)
                        System.out.println("Historical data path = " + ConfigurationParams.INPUT_DIR);
			}
		}
		if (i != args.length)
			System.err.println("Usage: ParseCmdLine [-verbose] [-xn] [-output afile] filename");
		else
			System.out.println("Success!");
	}
	
	
	
}
