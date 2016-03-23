package didclab.cse.buffalo.hysterisis;

import didclab.cse.buffalo.Partition;
import didclab.cse.buffalo.utils.csv.CSVReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;


public class Similarity {

  private static final Log LOG = LogFactory.getLog(Similarity.class);

  static double[] avgAttributes, variance;

  static double[] minSpecValues;
  static double[] maxSpecValues;
  static int skippedEntryCount = 0;
  private static double similarityThreshold = 0.999;

  public static List<Entry> readFile(String fname) {
    List<Entry> entries = new LinkedList<Entry>();
    try {
      File freader = new File(fname);
      String fileName = freader.getName();
      CSVReader reader = new CSVReader(new FileReader(fname), ',');
      //System.out.println("Reading "+fname+"...");
      //read line by line
      String[] header = reader.readNext();
      Map<String, Integer> attributeIndices = new HashMap<String, Integer>();
      for (int i = 0; i < header.length; i++)
        attributeIndices.put(header[i], i);
      int id = 0;
      String[] record = null;
      while ((record = reader.readNext()) != null) {
        Entry entry = new Entry();
        try {
          //Mandatory attributes
          if (attributeIndices.containsKey("Duration") && !record[attributeIndices.get("Duration")].isEmpty() &&
                  Double.parseDouble(record[attributeIndices.get("Duration")]) < 10) {
            skippedEntryCount++;
            continue;
          } else if (Double.parseDouble(record[attributeIndices.get("Throughput")]) > 9000) {
            skippedEntryCount++;
            continue;
          }
          entry.setId(id++);
          entry.setFileSize(Double.parseDouble(record[attributeIndices.get("FileSize")]));
          entry.setFileCount(Integer.parseInt(record[attributeIndices.get("FileCount")]));
          entry.setSource(record[attributeIndices.get("Source")]);
          entry.setDestination(record[attributeIndices.get("Destination")]);
          entry.setBandwidth(Double.parseDouble(record[attributeIndices.get("Bandwidth")]));
          //if(entry.getBandwidth() >= Math.pow(10, 10))
          //	entry.setBandwidth( entry.getBandwidth()*1024*1024*1024.0 );
          entry.setRtt(Double.parseDouble(record[attributeIndices.get("RTT")]));
          entry.setBufferSize(Double.parseDouble(record[attributeIndices.get("BufferSize")]));
          if (record[attributeIndices.get("Parallelism")].compareTo("na") == 0) {
            entry.setParallellism(1);
          } else {
            entry.setParallellism(Integer.parseInt(record[attributeIndices.get("Parallelism")]));
          }
          if (record[attributeIndices.get("Concurrency")].compareTo("na") == 0) {
            entry.setConcurrency(1);
          } else {
            entry.setConcurrency(Integer.parseInt(record[attributeIndices.get("Concurrency")]));
          }
          if (record[attributeIndices.get("Pipelining")].compareTo("na") == 0) {
            entry.setPipelining(0);
          } else {
            entry.setPipelining(Integer.parseInt(record[attributeIndices.get("Pipelining")]));
          }

          if (record[attributeIndices.get("Fast")].compareTo("ON") == 0 ||
                  record[attributeIndices.get("Fast")].compareTo("1") == 0) {
            entry.setFast(true);
          }

          //Optional attributes
          if (attributeIndices.containsKey(("TestBed"))) {
            entry.setTestbed(record[attributeIndices.get("TestBed")]);
          }
          entry.setThroughput(Double.parseDouble(record[attributeIndices.get("Throughput")]));
          if (attributeIndices.containsKey("Emulation")) {
            if (record[attributeIndices.get("Emulation")].compareTo("REAL") != 0) {
              entry.setEmulation(true);
            }
          }
          if (attributeIndices.containsKey("Dedicated")) {
            if (record[attributeIndices.get("Dedicated")].compareTo("true") != 0) {
              entry.setEmulation(true);
            }
          }
          if (attributeIndices.containsKey(("Note"))) {
            entry.setNote(record[attributeIndices.get("Note")]);
          }
          if (entry.getBandwidth() < Math.pow(10, 6)) {
            entry.setBandwidth(entry.getBandwidth() * Math.pow(10, 6));
          }
          entry.setDensity(Entry.findDensityOfList(entry.getFileSize(), entry.getBandwidth()));
          entry.setNote(fileName);
        } catch (Exception e) {
          for (String s : record)
            System.out.print(s + "\n");
          e.printStackTrace();
          System.exit(0);
        }
        entry.calculateSpecVector();
        entries.add(entry);
      }
      reader.close();
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(0);
    }
    return entries;
  }


  static public void normalizeDataset(List<Entry> entries) {
    double[] sumSpecVector = null;
    for (Entry entry : entries) {
      if (sumSpecVector == null) {
        sumSpecVector = new double[entry.specVector.size()];
      }
      for (int i = 0; i < sumSpecVector.length; i++) {
        sumSpecVector[i] += entry.specVector.get(i);
      }
    }

    Similarity.avgAttributes = new double[sumSpecVector.length];
    for (int i = 0; i < Similarity.avgAttributes.length; i++) {
      Similarity.avgAttributes[i] = 0.0;
    }
    System.out.println("Average attributes");
    for (int i = 0; i < sumSpecVector.length; i++) {
      Similarity.avgAttributes[i] = sumSpecVector[i] / entries.size();
      round(Similarity.avgAttributes[i], 3);
      System.out.print(Similarity.avgAttributes[i] + "*");
    }
    System.out.println();
    Similarity.variance = new double[sumSpecVector.length];
    for (int i = 0; i < Similarity.variance.length; i++) {
      Similarity.variance[i] = 0.0;
    }
    for (Entry e : entries) {
      for (int i = 0; i < e.specVector.size(); i++) {
        Similarity.variance[i] += Math.pow(e.specVector.get(i) - Similarity.avgAttributes[i], 2);
      }
    }
    System.out.println("Variance attributes");
    for (int i = 0; i < Similarity.variance.length; i++) {
      Similarity.variance[i] = Math.sqrt(Similarity.variance[i] / entries.size());
      System.out.print(Similarity.variance[i] + "*");
    }
    System.out.println();
    for (Entry e : entries) {
      for (int i = 0; i < e.specVector.size(); i++) {
        //double newValue = (e.specVector.get(i)-Similarity.avgAttributes[i]);
        double newValue = (e.specVector.get(i) / sumSpecVector[i]);
        //if(Similarity.variance[i] != 0 )
        //	newValue/= Similarity.variance[i];
        e.specVector.set(i, newValue);

      }
    }
  }


  static public void normalizeDataset2(List<List<Entry>> entries) {
    minSpecValues = entries.size() == 0 ? null : new double[entries.get(0).get(0).specVector.size()];
    maxSpecValues = entries.size() == 0 ? null : new double[entries.get(0).get(0).specVector.size()];
    Arrays.fill(minSpecValues, Double.POSITIVE_INFINITY);
    Arrays.fill(maxSpecValues, Double.NEGATIVE_INFINITY);
    for (List<Entry> entryList : entries) {
      for (Entry entry : entryList) {
        for (int i = 0; i < minSpecValues.length; i++) {
          if (entry.specVector.get(i) < minSpecValues[i]) {
            minSpecValues[i] = entry.specVector.get(i);
          }
          if (entry.specVector.get(i) > maxSpecValues[i]) {
            maxSpecValues[i] = entry.specVector.get(i);
          }
        }
      }
    }

    for (int i = 0; i < minSpecValues.length; i++) {
      LOG.info("Spec " + i + " Min:" + minSpecValues[i] + "Max:" + maxSpecValues[i]);
    }

    for (List<Entry> entryList : entries) {
      for (Entry e : entryList) {
        for (int i = 0; i < e.specVector.size(); i++) {
          if (maxSpecValues[i] == minSpecValues[i]) {
            e.specVector.set(i, Math.abs(e.specVector.get(i) - Similarity.minSpecValues[i]));
          } else {
            double newValue = (e.specVector.get(i) - minSpecValues[i]) / (maxSpecValues[i] - minSpecValues[i]);
            e.specVector.set(i, newValue);
          }
        }
      }
    }
  }

  static public void normalizeDataset3(List<List<Entry>> entries, List<Partition> chunks) {
    double[] maxValues = entries.size() == 0 ? null : new double[entries.get(0).get(0).specVector.size()];
    Arrays.fill(maxValues, Double.NEGATIVE_INFINITY);
    for (List<Entry> entryList : entries) {
      for (Entry entry : entryList) {
        for (int i = 0; i < maxValues.length; i++) {
          if (entry.specVector.get(i) > maxValues[i]) {
            maxValues[i] = entry.specVector.get(i);
          }
        }
      }
    }

    double[] ratios = new double[maxValues.length];
    for (int i = 0; i < maxValues.length; i++) {
      ratios[i] = 100 / maxValues[i];
    }

    for (List<Entry> entryList : entries) {
      for (Entry e : entryList) {

        for (int i = 0; i < e.specVector.size(); i++) {
          double newValue = e.specVector.get(i) * ratios[i];
          e.specVector.set(i, newValue);
        }
      }
    }

    for (int chunkNumber = 0; chunkNumber < chunks.size(); chunkNumber++) {
      Entry targetEntry = chunks.get(chunkNumber).entry;
      //Normalize values of target entry
      for (int i = 0; i < targetEntry.specVector.size(); i++) {
        double newValue = targetEntry.specVector.get(i) * ratios[i];
        targetEntry.specVector.set(i, newValue);
      }
    }


  }

  public static double round(double value, int places) {
    if (places < 0) {
      throw new IllegalArgumentException();
    }

    BigDecimal bd = new BigDecimal(value);
    bd = bd.setScale(places, RoundingMode.HALF_UP);
    return bd.doubleValue();
  }

  /*
   * This function takes list of entries and a target entry
   * Returns list of entries which is similar to target entry based on cosine similarity values
   */
  public static List<Entry> findSimilarEntries(List<List<Entry>> entries, Entry targetEntry) {
    Similarity similarity = new Similarity();
    similarity.measureCosineSimilarity(targetEntry, entries);
    List<Entry> mostSimilarEntries = new LinkedList<Entry>();

    int counter = 0;
    while (counter < 10000) {
      counter = 0;
      mostSimilarEntries.clear();
      for (List<Entry> entryList : entries) {
        for (Entry e : entryList) {
          if (e.getSimilarityValue() >= similarityThreshold) {
            mostSimilarEntries.add(e);
            counter++;
          }
        }
      }
      similarityThreshold -= 0.001;
    }
    //LogManager.writeToLog("Similarity threshold updated:"+similarityThreshold+" Count:"+counter, ConfigurationParams.STDOUT_ID);
    return mostSimilarEntries;
  }

  //sort entries based on date of the transfer
  public static void categorizeEntries(int chunkNumber, List<List<Entry>> trials,
                                       List<Entry> similarEntries) {
    //trials = new LinkedList<Map<String,Similarity.Entry>>();
    Set<String> set = new HashSet<String>();
    LinkedList<Entry> list = new LinkedList<Entry>();

    //Collections.sort(similarEntries, new DateComparator());
    Entry prev = similarEntries.get(0);
    for (Entry e : similarEntries) {
      //if(e.getFast() == false)	// IGNORE FAST DISABLED OPTIONS
      //	continue;
      /* Partition entries in two conditions:
			 * 1. Entry's network or data set characteristics is seen for the first time
			 * 2. Already seen entry type's repeating parameter values
			 */
      if (e.getIdentity().compareTo(prev.getIdentity()) != 0 || (set.contains(e.getParameters()) &&
              e.getParameters().compareTo(prev.getParameters()) != 0 && list.size() >= 6 * 6 * 6 - 1)) {
        //Entry s = set.get(e.getIdentity());
        //LogManager.writeToLog("New entry "+e.getSimilarityValue()+" "+e.printSpecVector()+" "+e.getIdentity()+" "
        //		+e.getThroughput()+" "+e.getParameters(), ConfigurationParams.STDOUT_ID);
        //Map<String,Similarity.Entry> copied = new HashMap<String,Similarity.Entry>(set);
        //trials.add((LinkedList)list.clone());


        if (list.size() >= 6 * 6 * 2) {
          trials.add(list);
          //LogManager.writeToLog("Adding "+list.size(), ConfigurationParams.STDOUT_ID);

        }
        list = new LinkedList<Entry>();
        set.clear();

      }
      //if(e.getDensity() == Density.LARGE)
      //	LogManager.writeToLog("Added:"+e.getIdentity()+" "+e.getThroughput()+" "+e.getDate().toString(), ConfigurationParams.STDOUT_ID);
      list.add(e);
      set.add(e.getParameters());
      prev = e;
    }
    // add the final list
    if (list.size() >= 6 * 6 * 2) {
      trials.add(list);
    }
    Collections.shuffle(trials);
    int trainingCount = Math.round(trials.size() * 0.8f);
    List<List<Entry>> training_trials = trials.subList(0, trainingCount);
    List<List<Entry>> testing_trials = trials.subList(trainingCount, trials.size());
    writeToFile("outputs/chunk_" + chunkNumber + "_training.txt", training_trials);
    writeToFile("outputs/chunk_" + chunkNumber + "_testing.txt", testing_trials);
  }

  private static void writeToFile(String fileName, List<List<Entry>> trials) {
    try {
      FileWriter writer = new FileWriter(fileName);
      writer.write(trials.size() + "\n");
      // Write metadata of each trial first.
      for (List<Entry> subset : trials)
        writer.write(subset.get(0).getNote() + " " + subset.size() + "\n");
      // Write entries of each trial
      for (int i = 0; i < trials.size(); i++) {
        List<Entry> subset = trials.get(i);
        for (Entry entry : subset) {
					/* Max parameters observed in the logs */
          int pipelining = entry.getPipelining() == -1 ? 0 : entry.getPipelining();
          int parallelism = entry.getParallellism() == -1 ? 1 : entry.getParallellism();
          int concurrency = entry.getConcurrency() == -1 ? 1 : entry.getConcurrency();
          concurrency = (int) (Math.min(entry.getConcurrency(), entry.getFileCount()));
          pipelining = (int) Math.min(entry.getPipelining(), (Math.max(entry.getFileCount() - entry.getConcurrency(), 0)));
          int fast = entry.getFast() == true ? 1 : 0;
          writer.write(concurrency + " " + parallelism + " " + pipelining + " " + fast + " " + entry.getThroughput() + "\n");
        }
        if (i != trials.size() - 1) {
          writer.write("*\n");
        }
      }
      writer.flush();
      writer.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public Map<Entry, Double> measureCosineSimilarity(Entry target, List<List<Entry>> entries) {

    Map<Entry, Double> cosineSimilarity = new HashMap<Entry, Double>();

    // List of spec vector elements
    //1-bandwidth
    //2-rtt
    //3-bandwidth*rtt/(8.0*bufferSize)
    //4-DensityToValue(density)*1.0
    //5
    //if (isDedicated) 	specVector.add(0.0)
    //else 				specVector.add(1.0)
    //6-fileSize/(1024*1024))---DISABLED
    //6-specVector.add(fileCount)
    //7- Testbed name

    double[] weights = {2, 2, 10, 10, 3, 1, 10};

		/*
		double sumWeight = 0;
		for (int i = 0; i < weights.length; i++) {
			sumWeight +=weights[i];
		}

		System.out.println("Weights:");
		for (int i = 0; i < weights.length; i++) {
			weights[i] = weights[i]/sumWeight;
			System.out.println(weights[i]);
		}
		 */
    //LogManager.createLogFile(ConfigurationParams.INFO_LOG_ID);

    double maxSimilarity = 0;
    Entry maxEntry = null;
    for (List<Entry> entryList : entries) {
      for (Entry e : entryList) {

        double similarityValue = 0;

				/*
      target.specVector.add(1.0);
			if(target.getTestbed() != null && target.getTestbed().compareTo(e.getTestbed()) == 0)
				e.specVector.add(1.0);
			else
				e.specVector.add(0.0);
				 */

        //Cosine Similarity
        double squareOne = 0, squareTwo = 0, multiplication = 0;

        for (int i = 0; i < e.specVector.size(); i++) {
          double value1 = e.specVector.get(i) * weights[i];
          double value2 = target.specVector.get(i) * weights[i];
          squareOne += (value1 * value1);
          squareTwo += (value2 * value2);
          multiplication += (value1 * value2);
        }
        similarityValue = multiplication / (Math.sqrt(squareOne) * Math.sqrt(squareTwo));
        if (similarityValue > maxSimilarity) {
          maxSimilarity = similarityValue;
          maxEntry = e;
        }
        //e.specVector.remove(e.specVector.size()-1);
        //target.specVector.remove(target.specVector.size()-1);
        if (similarityValue < 0.01) {
          LOG.fatal("Unexpected similarity value:" + similarityValue);
          for (int i = 0; i < e.specVector.size(); i++) {
            LOG.fatal(e.specVector.get(i) +
                    "\t" + target.specVector.get(i));
          }
          e.printEntry("");
          target.printEntry("");
          System.exit(-1);
        }
        /*
        if(e.getDensity() == target.getDensity() && similarityValue < .4){
					LOG.info("Unexpected similarity value:"+similarityValue);
					e.printEntry("");
					target.printEntry("");
					for (int i = 0; i < e.specVector.size(); i++) {
						LOG.info(e.specVector.get(i) +
								"\t" + target.specVector.get(i));
					}

					LogManager.writeToLog(e.getNote() + " "+ e.getDensity()+" " +e.getFileSize() + " " +
						similarityValue, ConfigurationParams.INFO_LOG_ID);
					System.exit(-1);
				}
				*/

        //e.specVector.remove(e.specVector.size()-1);
        //End of cosine-similarity

				/*
      //Pearson-correlation
			double squareOne = 0, squareTwo= 0 , multiplication = 0;
			double total1= 0 , total2 = 0;
			for (int i = 0; i < e.specVector.size(); i++) {
				total1 += e.specVector.get(i);
				total2 += target.specVector.get(i);
			}
			double mean1 = total1/e.specVector.size();
			double  mean2 = total2/e.specVector.size();
			for (int i = 0; i < e.specVector.size(); i++) {
				double value1 = e.specVector.get(i)-mean1;
				double value2 = target.specVector.get(i)-mean2;
				squareOne += (value1 * value1);
				squareTwo += (value2 * value2);
				multiplication += (value1 * value2);

			}
			double similarityValue = multiplication/(Math.sqrt(squareOne)*Math.sqrt(squareTwo));
			//Pearson-correlation


			 if(e.throughput == 108.929651069 || e.throughput == 8531.8550352391 || e.throughput ==1918.02612453
					|| e.throughput ==3340.78852179){
				.printEntry(e, "");
				System.out.println("mult:"+multiplication+"\tsquareOne:"+squareOne+"\tsquareTwo:"+squareTwo+"\tcosine:"+similarityValue);
			}
			/*if(e.testbed.compareTo("XSEDE") == 0 && e.fileSize> Math.pow(10, 9)){
				System.out.println(e.testbed + "\t"+ e.source+"\t"+e.destination+"\tDensity:"+e.density
		        		+"\tFileSize"+e.fileSize+"\tFileCount:"+e.fileCount+"\tSimilarity"+similarityValue);
			}*/
        e.similarityValue = similarityValue;
        //cosineSimilarity.put(e, similarityValue);
      }
    }
    //System.exit(-1);
    //maxEntry.printEntry(Double.toString(maxSimilarity));
    //LogManager.writeToLog("Max Entry:"+maxEntry.printEntry(Double.toString(maxSimilarity)), ConfigurationParams.STDOUT_ID);

    Similarity.similarityThreshold = maxSimilarity;
    //System.out.println("similarity size:"+cosineSimilarity.size());
    //ValueComparator bvc =  new ValueComparator(cosineSimilarity);
    //TreeMap<Entry,Double> sorted_map = new TreeMap<Entry,Double>(bvc);
    //sorted_map.putAll(cosineSimilarity);
    //System.out.println("sorted size:"+sorted_map.size());
    //		return sorted_map;
    return cosineSimilarity;
  }

  static class DateComparator implements Comparator<Entry> {

    // Note: this comparator imposes orderings that are inconsistent with equals.
    @Override
    public int compare(Entry a, Entry b) {
      if (a.getDate().before(b.getDate())) {
        return -1;
      } else {
        return 1;
      }
    }
  }

  class ValueComparator implements Comparator<Entry> {

    Map<Entry, Double> base;

    public ValueComparator(Map<Entry, Double> base) {
      this.base = base;
    }

    // Note: this comparator imposes orderings that are inconsistent with equals.
    public int compare(Entry a, Entry b) {
      if (Math.abs((base.get(a) - base.get(b))) < 0.0000001) {
        if (a.getThroughput() > b.getThroughput()) {
          return -1;
        } else {
          return 1;
        }
      } else if (base.get(a) > base.get(b)) {
        return -1;
      } else {
        return 1;
      } // returning 0 would merge keys
    }
  }


}
