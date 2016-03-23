package didclab.cse.buffalo;

import didclab.cse.buffalo.hysterisis.Entry;
import stork.util.XferList;

/**
 * Represents a calculated partition. A partition contains the centroid and
 * the data points.
 */
public class Partition {
  public Entry entry;
  /*variables for log based approach*/
  //Observed max values for each parameters in the log files
  int maxCC = Integer.MIN_VALUE, maxP = Integer.MIN_VALUE, maxPPQ = Integer.MIN_VALUE;
  /* centroid of the partition */
  private double centroid;
  private double samplingTime;
  private long samplingSize;
  private int[] samplingParameters;
  /* records belonging to this partition */
  private XferList fileList = new XferList("", "");

  /**
   * @return the density
   */

  public Partition(double centroid) {
    this.centroid = centroid;
  }

  public Partition() {
    this.centroid = 0;
    entry = new Entry();
  }

  public int[] getSamplingParameters() {
    return samplingParameters;
  }

  public void setSamplingParameters(int[] samplingParameters) {
    this.samplingParameters = samplingParameters;
  }

  /**
   * @return the maxCC
   */
  public int getMaxCC() {
    return maxCC;
  }

  /**
   * @param maxCC the maxCC to set
   */
  public void setMaxCC(int maxCC) {
    this.maxCC = maxCC;
  }

  /**
   * @return the maxP
   */
  public int getMaxP() {
    return maxP;
  }

  /**
   * @param maxP the maxP to set
   */
  public void setMaxP(int maxP) {
    this.maxP = maxP;
  }

  /**
   * @return the maxPPQ
   */
  public int getMaxPPQ() {
    return maxPPQ;
  }

  /**
   * @param maxPPQ the maxPPQ to set
   */
  public void setMaxPPQ(int maxPPQ) {
    this.maxPPQ = maxPPQ;
  }

  public void setEntry(Entry e) {
    entry.setBandwidth(e.getBandwidth());
    entry.setRtt(e.getRtt());
    entry.setBDP(e.getBDP());
    entry.setBufferSize(e.getBufferSize());
    entry.setDedicated(e.isDedicated());
  }

  public double getCentroid() {
    if (fileList.count() == 0) {
      return 0;
    }
    return fileList.size() / fileList.count();
  }

  public XferList getRecords() {
    return fileList;
  }

  public void setXferList(XferList fileList) {
    this.fileList = fileList;
  }

  public void addRecord(XferList.Entry e) {
    fileList.add(e.path, e.size);
  }

  /**
   * Calculate the new centroid from the data points. Mean of the
   * data points belonging to this partition is the new centroid.
   */
  public double calculateCentroid() {
    return fileList.avgFileSize();
  }

  /**
   * Clears the data records
   */
  public void reset() {
    // now clear the records
    fileList.removeAll();
  }

  public long getTotalSize() {
    return fileList.size();
  }

  public double getSamplingTime() {
    return samplingTime;
  }

  public void setSamplingTime(double samplingTime) {
    this.samplingTime = samplingTime;
  }

  public long getSamplingSize() {
    return samplingSize;
  }

  public void setSamplingSize(long samplingSize) {
    this.samplingSize = samplingSize;
  }
}
