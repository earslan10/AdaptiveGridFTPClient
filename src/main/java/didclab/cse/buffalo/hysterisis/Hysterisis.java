package didclab.cse.buffalo.hysterisis;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.annotations.VisibleForTesting;

import didclab.cse.buffalo.ConfigurationParams;
import didclab.cse.buffalo.Partition;
import didclab.cse.buffalo.log.LogManager;
import didclab.cse.buffalo.utils.Utils;
import matlabcontrol.MatlabProxy;
import matlabcontrol.MatlabProxyFactory;
import matlabcontrol.MatlabProxyFactoryOptions;
import stork.module.CooperativeModule.GridFTPTransfer;
import stork.util.XferList;

public class Hysterisis {
	
	private static final Log LOG = LogFactory.getLog(Hysterisis.class);

	List<List<Entry>> entries;
	private GridFTPTransfer gridFTPClient;
	
	public Hysterisis(GridFTPTransfer gridFTPClient) {
		// TODO Auto-generated constructor stub
		this.gridFTPClient = gridFTPClient;
	}
	
	@VisibleForTesting
	public void setGridFTP(GridFTPTransfer gridFTPClient) {
		this.gridFTPClient = gridFTPClient;
	}
	
	/**
	 * @return the entries
	 */
	public List<List<Entry>> getEntries() {
		return entries;
	}
	 
	
	public void parseInputFiles(){
		File folder = new File(ConfigurationParams.INPUT_DIR);
		File[] listOfFiles = folder.listFiles();
		List<String> historicalDataset = new ArrayList<>(listOfFiles.length);
		entries = new ArrayList<List<Entry>>();
		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile()) {
				historicalDataset.add(ConfigurationParams.INPUT_DIR + listOfFiles[i].getName());
		    }
		}
		for (int i = 0; i < historicalDataset.size(); i++) {
			String fileName = historicalDataset.get(i);
			List<Entry> fileEntries = Similarity.readFile(fileName);
			if(!fileEntries.isEmpty())
				entries.add(fileEntries);
		}
		//System.out.println("Skipped Entry count =" + Similarity.skippedEntryCount);
	}
	
	public void transfer(List<Partition> chunks, Entry intendedTransfer, XferList dataset){
		parseInputFiles();
		if(entries.isEmpty()){	// Make sure there are log files to run hysterisis 
			LOG.fatal("No input entries found to run hysterisis analysis. Exiting...");
		}
		double []sampleThroughputs = new double[chunks.size()];
		// Run sample transfer to learn current network load
		// Create sample dataset to and transfer to measure current load on the network
    	for (int chunkNumber = 0 ; chunkNumber < chunks.size() ; chunkNumber++) {
    		Partition chunk =  chunks.get(chunkNumber);

    		/*
    		List<Entry> similarEntries = Similarity.findSimilarEntries(hysterisis.getEntries(), chunks.get(chunkNumber).entry);
			//LogManager.writeToLog("The number of similar entries is:"+similarEntries.size(), ConfigurationParams.STDOUT_ID);
	    	//Categorize selected entries based on log date
	    	LinkedList<LinkedList<Entry>> trials = new LinkedList<LinkedList<Entry>>();
	    	Similarity.categorizeEntries(chunkNumber, trials, similarEntries);
	    	
	    	if(ConfigurationParams.USE_HISTORY)
	    		continue;
	    	*/
    		
        	XferList sample_files = new XferList("", "") ;
        	double MINIMUM_SAMPLING_SIZE = 40 * intendedTransfer.getBDP();
        	while (sample_files.size() < MINIMUM_SAMPLING_SIZE || sample_files.count() < 2){ 
        		XferList.Entry file = chunk.getRecords().pop();
        		sample_files.add(file.path, file.size);
        		//LogManager.writeToLog(file.path(), ConfigurationParams.STDOUT_ID);
        	}
        	
        	sample_files.sp = dataset.sp;
        	sample_files.dp = dataset.dp;
        	int [] samplingParams = Utils.getBestParams(sample_files);
			// use higher concurrency values for sampling to be able to 
        	// observe available throughput better
        	int cc = samplingParams[0];
			cc = cc > 8 ? cc : Math.min(8, sample_files.count());
			samplingParams[0] = cc;
			chunk.setSamplingSize(sample_files.size());
        	chunk.setSamplingParameters(samplingParams);
			long start = System.currentTimeMillis();
			//LOG.info("Sample transfer called at "+ManagementFactory.getRuntimeMXBean().getUptime());
		    sampleThroughputs[chunkNumber] = transferList(sample_files, chunk.getSamplingParameters());
			
		    chunk.setSamplingTime((System.currentTimeMillis()-start)/1000.0);
		    //LOG.info( chunk.getSamplingTime() + " "+  chunk.getSamplingSize());
		    //TODO: handle transfer failure 
		    if(sampleThroughputs[chunkNumber] == -1)System.exit(-1);	
    	}

    	// Based on input files and sample tranfer throughput; categorize logs and fit model
    	// Then find optimal parameter values out of the model
    	Object[][] results = runMatlabModeling(chunks, sampleThroughputs);
    	//if(ConfigurationParams.USE_HISTORY)
    	//	System.exit(-1);
    	if (results == null)
    		return;
		for (int  i=0; i< results.length; i++){
			Object []result = results[i];
			
			double [] paramValues = (double []) result[0];
			double estimatedThroughput = ((double []) result[1]) [0];
			double accuracy = ((double []) result[2]) [0];
			
			//Convert from double to int
			int []parameters = new int[paramValues.length+1];
			for(int j = 0; j<paramValues.length; j++)
				parameters[j] = (int)paramValues[j];
			parameters[paramValues.length] = (int)intendedTransfer.getBufferSize();

			LOG.info("Estimated params cc:"+paramValues[0]+" p:"+paramValues[1]+
					" ppq:"+paramValues[2]+" throughput:"+estimatedThroughput+" Accuracy:"+accuracy);
			Partition chunk = chunks.get(i);
			long totalDataSize = chunk.getTotalSize() + chunk.getSamplingSize();
			long start = System.currentTimeMillis();
			// Total data size in this chunk type including sampling transfers
			
			transferList(chunk.getRecords(), parameters);
			double totalTime = (System.currentTimeMillis()-start)/1000.0 + chunk.getSamplingTime();
			LOG.info("Average throughput of chunk " +i+ " " + ((totalDataSize*8)/(totalTime*1000*1000)));
		}
    	
	}
	
	/*
	 * Run sample data transfer to learn current network load
	 * sample_files is supposed to be set of files from highest density chunk
	 */
	public double transferList(XferList sample_files, int[] parameters) {
		
		
    	//parallelism, pipelining and concurrency parameters 
		if(parameters == null)
			parameters = Utils.getBestParams(sample_files);
    	
    	double measuredThroughput = -1;
    	try{
	    	//Run sampling transfer
	    	LOG.info("Transferring chunk --> files:"+sample_files.count()+
	    			   " Centroid:"+ Utils.printSize(sample_files.size()/sample_files.count(), true)+
	    		 	   " total:"+ Utils.printSize(sample_files.size(), true)+"\tppq:"+parameters[2]+"*p:"+
	    		 	  parameters[1]+"*"+"*cc:"+parameters[0]);
	    	measuredThroughput = gridFTPClient.runTransfer(parameters[2], parameters[1], 
	    			parameters[0], parameters[3], sample_files);
    	}
    	catch(Exception e){
    		e.printStackTrace();
    		System.exit(0);
    	}
		return measuredThroughput;
	}
	
	/*
	 * 1-It first categorizes the historical dataset based on similarity to each chunk
	 * 2- Write each set of entries to the files
	 * 3-Run polyfit matlab function to derive model and find optimal point that yields maximum throughput
	 */
	public Object[][] runMatlabModeling(List<Partition>  chunks, double []sampleThroughputs){
		int []setCounts = new int[chunks.size()];
		Similarity.normalizeDataset3(entries, chunks);
		//LOG.info("Entries are normalized at "+ ManagementFactory.getRuntimeMXBean().getUptime());
		for (int chunkNumber = 0 ; chunkNumber < chunks.size() ; chunkNumber++) {
			List<Entry> similarEntries = Similarity.findSimilarEntries(entries, chunks.get(chunkNumber).entry);
	    	//Categorize selected entries based on log date
	    	List<List<Entry>> trials = new LinkedList<List<Entry>>();
	    	Similarity.categorizeEntries(chunkNumber, trials, similarEntries);
	    	setCounts[chunkNumber] = trials.size();
			//LOG.info("Chunk "+chunkNumber + " entries are categorized and written to disk at "+ jvmUpTime);
	    }
		/*
    	 * Run matlab optimization to find set of "optimal" parameters for each chunk 
    	 */
		//return null;
    	return polyFitbyMatlab(chunks, setCounts, sampleThroughputs);
	}
	
	/*
	 * For each chunk, run model fitting (polyfit) function then extract 
	 * combination of cc,p and ppq values for best throughput
	 * 
	 * Inputs: chunk-- file groups partitioned based on file size
	 * 		   logFileCount-- the number of set of logs (based on date) that are similar to chunks characteristics
	 * 		   sampleThroughputs-- sample throughput results obtained by transferring small piece of each chunks
	 * 		   maxParams---- For each chunk, maximum observed cc, p and ppq values in logs
	 * Output: results-- it holds estimated values of cc,p and ppq for each chunk set
	 */
	public Object [][] polyFitbyMatlab(List<Partition>  chunks, int []logFilesCount,
			double []sampleThroughputs){
		Object[][] results = null;
		MatlabProxyFactoryOptions options = new MatlabProxyFactoryOptions.Builder()
        .setUsePreviouslyControlledSession(true)
        .setHidden(true)
        .setMatlabLocation(ConfigurationParams.MATLAB_DIR+"/matlab")
        .build();
    	MatlabProxyFactory factory = new MatlabProxyFactory(options);
    	MatlabProxy proxy = null;
    	try {
    		proxy = factory.getProxy();
    		if(proxy == null){
    			LogManager.writeToLog("Matlab connection is not valid", ConfigurationParams.STDOUT_ID);
    			return null;
    		}
    		results = new Object[chunks.size()][];
    		LOG.info("chunks :" + chunks.size());
    		for (int chunkNumber = 0 ; chunkNumber < chunks.size() ; chunkNumber++) {
		    	int []sampleTransferValues = chunks.get(chunkNumber).getSamplingParameters();
				proxy.eval("cd "+ConfigurationParams.MATLAB_SCRIPT_DIR);
				String filePath = ConfigurationParams.OUTPUT_DIR + "/chunk_" + chunkNumber;
				String command = "analyzeAndEvaluate('"+filePath+"',"+sampleThroughputs[chunkNumber]+
								  ",["+sampleTransferValues[0]+","+sampleTransferValues[1] +
								  ","+sampleTransferValues[2]+"]"+")";
				LOG.info("Matlab command:" + command);
				results[chunkNumber] = proxy.returningEval(command,3);
    		}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	proxy.disconnect();
    	return results;
	}
	
}
