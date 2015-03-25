package didclab.cse.buffalo.hysterisis;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import matlabcontrol.MatlabProxy;
import matlabcontrol.MatlabProxyFactory;
import matlabcontrol.MatlabProxyFactoryOptions;
import didclab.cse.buffalo.ConfigurationParams;
import didclab.cse.buffalo.CooperativeChannels;
import didclab.cse.buffalo.Partition;
import didclab.cse.buffalo.log.LogManager;

public class Hysterisis {
	List<Entry> entries;
	List<String> historicalDataset;
	
	public Hysterisis() {
		// TODO Auto-generated constructor stub
		entries = new ArrayList<Entry>();
		historicalDataset= new ArrayList<String>();
	}
	
	
	
	/**
	 * @return the entries
	 */
	public List<Entry> getEntries() {
		return entries;
	}



	/**
	 * @param entries the entries to set
	 */
	public void setEntries(List<Entry> entries) {
		this.entries = entries;
	}



	/**
	 * @return the historicalDataset
	 */
	public List<String> getHistoricalDataset() {
		return historicalDataset;
	}



	/**
	 * @param historicalDataset the historicalDataset to set
	 */
	public void setHistoricalDataset(List<String> historicalDataset) {
		this.historicalDataset = historicalDataset;
	}



	public List<String> getInputFiles(){
		return historicalDataset;
	}
	 
	
	public void parseInputFiles(){
		//historicalDataset.add("xsede.csv");
		//historicalDataset.add("loni.csv");
		//historicalDataset.add("futuregrid.csv");
		//historicalDataset.add("emulab.csv");
		//historicalDataset.add("didclab.csv");
		historicalDataset.add(ConfigurationParams.INPUT_DIR + "sg0.25-1M.csv");
		historicalDataset.add(ConfigurationParams.INPUT_DIR + "sg5-25M.csv");
		historicalDataset.add(ConfigurationParams.INPUT_DIR + "sg100M.csv");
		historicalDataset.add(ConfigurationParams.INPUT_DIR + "sg1G.csv");
		historicalDataset.add(ConfigurationParams.INPUT_DIR + "sg3G.csv");
		
		for (int i = 0; i < historicalDataset.size(); i++) {
			String fileName = historicalDataset.get(i);
			//Similarity.readFile(entries, fileName);
			Similarity.readFile(entries, fileName);
		}
		LogManager.writeToLog("Total Entries"+entries.size(), ConfigurationParams.STDOUT_ID);
		Similarity.normalizeDataset2(entries);
		
	}
	
	/*
	 * 1-It first categorizes the historical dataset based on similarity to each chunk
	 * 2- Write each set of entries to the files
	 * 3-Run polyfit matlab function to derive model and find optimal point that yields maximum throughput
	 */
	public Object[][] runMatlabModeling(ArrayList<Partition>  chunks, double []sampleThroughputs){
		
		//hold maximum parallelism, pipelining, concurrency values observed in the dataset
		int [][] maxValuesInDataset = new int[chunks.size()][3];
		int []setCounts = new int[chunks.size()];
		for (int chunkNumber = 0 ; chunkNumber < chunks.size() ; chunkNumber++) {
			List<Entry> similarEntries = Similarity.findSimilarEntries(entries, chunks.get(chunkNumber).entry);
			
	    	//Categorize selected entries based on log date
	    	LinkedList<LinkedList<Entry>> trials = new LinkedList<LinkedList<Entry>>();
	    	maxValuesInDataset[chunkNumber] = Similarity.categorizeEntries(chunkNumber, trials, similarEntries);
	    	setCounts[chunkNumber] = trials.size();
	    }
		/*
    	 * Run matlab optimization to find set of "optimal" parameters for each chunk 
    	 */
    	return polyFitbyMatlab(chunks, setCounts, sampleThroughputs, maxValuesInDataset);
		
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
	public Object [][] polyFitbyMatlab(ArrayList<Partition>  chunks, int []logFilesCount, double []sampleThroughputs, int[][] maxParams){
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
    		for (int chunkNumber = 0 ; chunkNumber < chunks.size() ; chunkNumber++) {
		    	int []sampleTransferValues = CooperativeChannels.getBestParams(chunks.get(chunkNumber).getRecords());
				proxy.eval("cd "+ConfigurationParams.MATLAB_SCRIPT_DIR);
				String command = "main("+chunkNumber+","+sampleThroughputs[chunkNumber]+","+(logFilesCount[chunkNumber]-1)+
								  ",["+sampleTransferValues[0]+","+sampleTransferValues[1]+","+sampleTransferValues[2]+"]"+
								  ",["+maxParams[chunkNumber][0]+","+maxParams[chunkNumber][1]+","+
								  maxParams[chunkNumber][2]+"]"+", '"+ConfigurationParams.OUTPUT_DIR+"')";
				LogManager.writeToLog("\t"+command, ConfigurationParams.INFO_LOG_ID, ConfigurationParams.STDOUT_ID);
				results[chunkNumber] = proxy.returningEval(command,2);
    		}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	proxy.disconnect();
    	return results;
	}
	
}
