/**
 * 
 */
package didclab.cse.buffalo.hysterisis;

import java.util.Date;
import java.util.Vector;

import didclab.cse.buffalo.ConfigurationParams;
import didclab.cse.buffalo.CooperativeChannels.Density;
import didclab.cse.buffalo.log.LogManager;

/**
 * @author earslan
 *
 */
public class Entry{
	 //public static enum Density{SMALL, MIDDLE, LARGE, HUGE} ;
	 
	 private int id;
	 private String testbed;
	 private String source, destination;
	 private double bandwidth;
	 private double RTT;
	 private double BDP;
	 private double bufferSize;
	 private double fileSize;
	 private double fileCount;
	 private Density density;
	 private double totalDatasetSize;
	 private Date date;
	 private double throughput, duration;
	 private  int parallellism, pipelining, concurrency;
	 private  boolean fast;
	 private boolean isEmulation;
	 private boolean isDedicated;
	 private String note;
	 private int maxConcurrency;
	
	double similarityValue;
	 Vector<Double> specVector;
	 
	 public Entry(){
		 date = new Date();
		 isDedicated = false;
		 isEmulation = false;
		 fast = false;
	 }
	 
	 /**
	 * @return the id
	 */
	public int getId() {
		return id;
	}

	/**
	 * @param id the id to set
	 */
	public void setId(int id) {
		this.id = id;
	}

	/**
	 * @return the testbed
	 */
	public String getTestbed() {
		return testbed;
	}

	/**
	 * @param testbed the testbed to set
	 */
	public void setTestbed(String testbed) {
		this.testbed = testbed;
	}

	/**
	 * @return the source
	 */
	public String getSource() {
		return source;
	}

	/**
	 * @param source the source to set
	 */
	public void setSource(String source) {
		this.source = source;
	}

	/**
	 * @return the destination
	 */
	public String getDestination() {
		return destination;
	}

	/**
	 * @param destination the destination to set
	 */
	public void setDestination(String destination) {
		this.destination = destination;
	}

	/**
	 * @return the bandwidth
	 */
	public double getBandwidth() {
		return bandwidth;
	}

	/**
	 * @param bandwidth the bandwidth to set
	 */
	public void setBandwidth(double bandwidth) {
		this.bandwidth = bandwidth;
	}

	/**
	 * @return the rtt
	 */
	public double getRtt() {
		return RTT;
	}

	/**
	 * @param rtt the rtt to set
	 */
	public void setRtt(double rtt) {
		this.RTT = rtt;
	}

	/**
	 * @return the bDP
	 */
	public double getBDP() {
		return BDP;
	}

	/**
	 * @param bDP the bDP to set
	 */
	public void setBDP(double BDP) {
		this.BDP = BDP;
	}

	/**
	 * @return the bufferSize
	 */
	public double getBufferSize() {
		return bufferSize;
	}

	/**
	 * @param bufferSize the bufferSize to set
	 */
	public void setBufferSize(double bufferSize) {
		this.bufferSize = bufferSize;
	}

	/**
	 * @return the fileSize
	 */
	public double getFileSize() {
		return fileSize;
	}

	/**
	 * @param fileSize the fileSize to set
	 */
	public void setFileSize(double fileSize) {
		this.fileSize = fileSize;
	}

	/**
	 * @return the fileCount
	 */
	public double getFileCount() {
		return fileCount;
	}

	/**
	 * @param fileCount the fileCount to set
	 */
	public void setFileCount(double fileCount) {
		this.fileCount = fileCount;
	}

	/**
	 * @return the density
	 */
	public Density getDensity() {
		return density;
	}

	/**
	 * @param density the density to set
	 */
	public void setDensity(Density density) {
		this.density = density;
	}

	/**
	 * @return the totalDatasetSize
	 */
	public double getTotalDatasetSize() {
		return totalDatasetSize;
	}

	/**
	 * @param totalDatasetSize the totalDatasetSize to set
	 */
	public void setTotalDatasetSize(double totalDatasetSize) {
		this.totalDatasetSize = totalDatasetSize;
	}

	/**
	 * @return the date
	 */
	public Date getDate() {
		return date;
	}

	/**
	 * @param date the date to set
	 */
	public void setDate(Date date) {
		this.date = date;
	}

	/**
	 * @return the throughput
	 */
	public Double getThroughput() {
		return throughput;
	}

	/**
	 * @param throughput the throughput to set
	 */
	public void setThroughput(Double throughput) {
		this.throughput = throughput;
	}

	/**
	 * @return the duration
	 */
	public Double getDuration() {
		return duration;
	}

	/**
	 * @param duration the duration to set
	 */
	public void setDuration(Double duration) {
		this.duration = duration;
	}

	/**
	 * @return the parallellism
	 */
	public int getParallellism() {
		return parallellism;
	}

	/**
	 * @param parallellism the parallellism to set
	 */
	public void setParallellism(int parallellism) {
		this.parallellism = parallellism;
	}

	/**
	 * @return the pipelining
	 */
	public int getPipelining() {
		return pipelining;
	}

	/**
	 * @param pipelining the pipelining to set
	 */
	public void setPipelining(int pipelining) {
		this.pipelining = pipelining;
	}

	/**
	 * @return the concurrency
	 */
	public int getConcurrency() {
		return concurrency;
	}

	/**
	 * @param concurrency the concurrency to set
	 */
	public void setConcurrency(int concurrency) {
		this.concurrency = concurrency;
	}

	/**
	 * @return the fast
	 */
	public boolean getFast() {
		return fast;
	}

	/**
	 * @param fast the fast to set
	 */
	public void setFast(boolean fast) {
		this.fast = fast;
	}

	/**
	 * @return the isEmulation
	 */
	public boolean isEmulation() {
		return isEmulation;
	}

	/**
	 * @param isEmulation the isEmulation to set
	 */
	public void setEmulation(boolean isEmulation) {
		this.isEmulation = isEmulation;
	}

	/**
	 * @return the isDedicated
	 */
	public boolean isDedicated() {
		return isDedicated;
	}

	/**
	 * @param isDedicated the isDedicated to set
	 */
	public void setDedicated(boolean isDedicated) {
		this.isDedicated = isDedicated;
	}

	/**
	 * @return the note
	 */
	public String getNote() {
		return note;
	}

	/**
	 * @param note the note to set
	 */
	public void setNote(String note) {
		this.note = note;
	}

	/**
	 * @return the similarityValue
	 */
	public double getSimilarityValue() {
		return similarityValue;
	}

	/**
	 * @param similarityValue the similarityValue to set
	 */
	public void setSimilarityValue(double similarityValue) {
		this.similarityValue = similarityValue;
	}

	/**
	 * @return the specVector
	 */
	public Vector<Double> getSpecVector() {
		return specVector;
	}

	/**
	 * @param specVector the specVector to set
	 */
	public void setSpecVector(Vector<Double> specVector) {
		this.specVector = specVector;
	}

	 /**
	 * @return the maxConcurrency
	 */
	public int getMaxConcurrency() {
		return maxConcurrency;
	}

	/**
	 * @param maxConcurrency the maxConcurrency to set
	 */
	public void setMaxConcurrency(int maxConcurrency) {
		this.maxConcurrency = maxConcurrency;
	}

	
	public static Density findDensityOfList(Double averageFileSize, double BDP){
			if(averageFileSize < BDP/10)
				return Density.SMALL;
			else if(averageFileSize < BDP/2)
				return Density.MIDDLE;
			else if (averageFileSize < 20 * BDP)
				return Density.LARGE;
			return Density.HUGE;
	}	
	int DensityToValue(Density density){
		if(density == Density.SMALL)
			return 1;
		if(density == Density.MIDDLE)
			return 11;
		if(density == Density.LARGE)
			return 21;
		if(density == Density.HUGE)
			return 31;
		else return -1;
	}
	public void calculateSpecVector(){
		specVector = new Vector<Double>();
		//specVector.add(fileSize/(1024*1024*1024));
		//specVector.add(fileSize);
		specVector.add(bandwidth);
		specVector.add(RTT);
		specVector.add(bandwidth*RTT/(8.0*bufferSize));
		specVector.add(DensityToValue(density)*1.0);
		if(isDedicated)
			specVector.add(0.0);
		else
			specVector.add(1.0);
		//specVector.add(bufferSize);
		//specVector.add(bufferSize/(1024*1024));
		specVector.add(Math.log10(fileSize/(1024*1024)));
		specVector.add(Math.log10(fileCount)+1);
		
		
	}
	
	String getIdentity(){
		return note+"*"+fileSize+"*"+fileCount+"*"+density.name()+"*"+testbed+"*"+source+"*"+
				destination+"*"+bandwidth+"*"+RTT+"*"+bufferSize+"*"+parallellism+"*"+
				concurrency+"*"+pipelining+"*"+fast+"*"+isEmulation+"*"+
				isDedicated;
	}
	
	@Override
	public int hashCode() {
		//System.out.println("HashCode:"+getIdentity().hashCode());
		return getIdentity().hashCode();
	}
	
	public  void printEntry(String... extraInfo){
		LogManager.writeToLog(note+"*"+fileSize+"*"+fileCount+"*"+density.name()+"*"+testbed+"*"+source+"*"+
				destination+"*"+bandwidth+"*"+RTT+"*"+bufferSize+"*p:"+parallellism+"*cc:"+
				concurrency+"*ppq:"+pipelining+"*"+fast+"*"+throughput+"*"+isEmulation+"*"+
				isDedicated+" date:"+date.toString(), ConfigurationParams.STDOUT_ID);
	}
	public  void printSpecVector(){
		LogManager.writeToLog(specVector.toString(), ConfigurationParams.STDOUT_ID);
	}
}

