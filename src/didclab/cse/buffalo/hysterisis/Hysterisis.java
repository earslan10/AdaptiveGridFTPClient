package didclab.cse.buffalo.hysterisis;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import didclab.cse.buffalo.ConfigurationParams;
import didclab.cse.buffalo.CooperativeChannels;
import didclab.cse.buffalo.Partition;

public class Hysterisis {
	List<Entry> entries;
	List<String> historicalDataset;
	
	public Hysterisis() {
		// TODO Auto-generated constructor stub
		entries = new ArrayList<Entry>();
		historicalDataset= new ArrayList<String>();
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
		historicalDataset.add(ConfigurationParams.INPUT_DIR + "sg1G.csv");
		historicalDataset.add(ConfigurationParams.INPUT_DIR + "sg3G.csv");
		historicalDataset.add(ConfigurationParams.INPUT_DIR + "sg100M.csv");
		
		for (int i = 0; i < historicalDataset.size(); i++) {
			String fileName = historicalDataset.get(i);
			//Similarity.readFile(entries, fileName);
			Similarity.readFile(entries, fileName);
		}
		Similarity.normalizeDataset(entries);
		
	}
	
	public Object[][] runMatlabModeling(ArrayList<Partition>  chunks, double []sampleThroughputs){
		/*
    	 * Find equations for each chunk 
    	 */
		Object [][] results = new Object [chunks.size()][];
		ModellingAndOptimization matlabInstance = new ModellingAndOptimization();
		if(matlabInstance.initializeMatlab()){
	    	for (int chunkNumber = 0 ; chunkNumber < chunks.size() ; chunkNumber++) {
	    		Partition chunk =  chunks.get(chunkNumber);
				List<Entry> similarEntries = Similarity.findSimilarEntries(entries, CooperativeChannels.targetTransfer);
				
		    	//Categorize selected entries based on log date
		    	LinkedList<LinkedList<Entry>> trials = new LinkedList<LinkedList<Entry>>();
		    	
		    	int [] maxObservedValues = Similarity.categorizeEntries(chunkNumber, trials, similarEntries);
		    	int []sampleTransferValues = CooperativeChannels.getBestParams(chunk.getRecords());
		    		    	
		    	//Fit a model on selected historical data entries to derive a formula
		    	results[chunkNumber] = matlabInstance.polyFitbyMatlab(chunkNumber, trials.size() , sampleThroughputs[chunkNumber] , maxObservedValues, sampleTransferValues);
			}
		}
    	return results;
	}
}
