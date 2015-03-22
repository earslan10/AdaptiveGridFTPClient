package didclab.cse.buffalo;

import stork.util.XferList;

/**
 * Represents a calculated partition. A partition contains the centroid and
 * the data points.
 */
public class Partition {
    /* centroid of the partition */
    private double centroid;
    
    /*variables for log based approach*/
    //Observed max values for each parameters in the log files
    int maxCC = Integer.MIN_VALUE, maxP = Integer.MIN_VALUE, maxPPQ = Integer.MIN_VALUE;
    
    /* records belonging to this partition */
    private XferList fileList = new XferList("", "") ;

    public Partition(double centroid) {
        this.centroid = centroid;
    }
    public Partition() {
        this.centroid = 0;
    }

    public double getCentroid() {
        return centroid;
    }

    public XferList getRecords() {
        return fileList;
    }

    public void addRecord(XferList.Entry e) {
        fileList.add(e.path,e.size);
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
}
