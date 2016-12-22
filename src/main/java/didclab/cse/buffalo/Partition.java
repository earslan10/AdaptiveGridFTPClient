package didclab.cse.buffalo;

import didclab.cse.buffalo.hysterisis.Entry;
import didclab.cse.buffalo.utils.TunableParameters;
import stork.util.XferList;

import java.util.*;

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
  /* records belonging to this partition */
  private int chunkNumber;
  public boolean isReadyToTransfer = false;
  private XferList fileList = new XferList("", "");
  private TunableParameters tunableParameters;
  private List<TunableParameters> calculatedParametersSeries;
  private List<Double> calculatedThroughputSeries;

  /**
   * @return the density
   */

  public Partition(double centroid) {
    this.centroid = centroid;
  }

  public Partition() {
    this.centroid = 0;
    entry = new Entry();
    calculatedParametersSeries = new LinkedList<>();
    calculatedThroughputSeries = new LinkedList();
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
    fileList.addEntry(e);
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

  public int getChunkNumber() {
    return chunkNumber;
  }

  public void setChunkNumber(int chunkNumber) {
    this.chunkNumber = chunkNumber;
  }

  public void setTunableParameters (TunableParameters tunableParameters) {
    this.tunableParameters = tunableParameters;
  }

  public TunableParameters getTunableParameters() {
    return tunableParameters;
  }
  public void addToTimeSeries(TunableParameters tunableParameters, double throughput) {
    synchronized (calculatedParametersSeries) {
      calculatedParametersSeries.add(tunableParameters);
      calculatedThroughputSeries.add(throughput);
    }
    if (this.tunableParameters == null) {
      this.tunableParameters = tunableParameters;
      if (this.tunableParameters.getConcurrency() > fileList.count()) {
        this.tunableParameters.setConcurrency(fileList.count());
      }
    }
  }

  public List<TunableParameters> getLastNFromSeries (int n) {
    int count = Math.min(calculatedParametersSeries.size(), n);
    List<TunableParameters> parametersSeries = new LinkedList<>();
    synchronized (calculatedParametersSeries) {
      for (int i = 0; i < count; i++) {
        parametersSeries.add(calculatedParametersSeries.get(calculatedParametersSeries.size()- 1 - i));
      }
      return parametersSeries;
    }
  }

  public void popFromSeries() {
    calculatedParametersSeries.remove(calculatedParametersSeries.size() -1);
    calculatedThroughputSeries.remove(calculatedThroughputSeries.size() -1);
  }

  public int getCountOfSeries() {
    return calculatedParametersSeries.size();
  }
}
