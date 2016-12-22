package didclab.cse.buffalo.utils;

import didclab.cse.buffalo.CooperativeChannels;
import didclab.cse.buffalo.CooperativeChannels.Density;
import didclab.cse.buffalo.hysterisis.Entry;
import stork.module.CooperativeModule;
import stork.util.XferList;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.LinkedList;
import java.util.List;

public class Utils {

  public static String printSize(double random, boolean inByte) {
    char sizeUnit = 'b';
    if (inByte) {
      sizeUnit = 'B';
    }
    DecimalFormat df = new DecimalFormat("###.##");
    if (random < 1024.0) {
      return df.format(random) + " " + sizeUnit;
    } else if (random < 1024.0 * 1024) {
      return df.format(random / 1024.0) + " K" + sizeUnit;
    } else if (random < 1024.0 * 1024 * 1024) {
      return df.format(random / (1024.0 * 1024)) + " M" + sizeUnit;
    } else if (random < (1024 * 1024 * 1024 * 1024.0)) {
      return df.format(random / (1024 * 1024.0 * 1024)) + " G" + sizeUnit;
    } else {
      return df.format(random / (1024 * 1024 * 1024.0 * 1024)) + " T" + sizeUnit;
    }
  }

  public static TunableParameters getBestParams(XferList xl) {
    Density density = Entry.findDensityOfList(xl.avgFileSize(), CooperativeChannels.intendedTransfer.getBandwidth());
    xl.density = density;
    double avgFileSize = xl.avgFileSize();
    int fileCountToFillThePipe = (int) Math.round(CooperativeChannels.intendedTransfer.getBDP() / avgFileSize);
    int pLevelToFillPipe = (int) Math.ceil(CooperativeChannels.intendedTransfer.getBDP() / CooperativeChannels.intendedTransfer.getBufferSize());
    int pLevelToFillBuffer = (int) Math.ceil(avgFileSize / CooperativeChannels.intendedTransfer.getBufferSize());
    int cc = Math.min(Math.min(Math.max(fileCountToFillThePipe, 2), xl.count()), CooperativeChannels.intendedTransfer.getMaxConcurrency());
    int ppq = fileCountToFillThePipe;
    int p = Math.max(Math.min(pLevelToFillPipe, pLevelToFillBuffer), 1);
    p = avgFileSize > CooperativeChannels.intendedTransfer.getBDP() ? p : p;
    return new TunableParameters.Builder()
        .setConcurrency(cc)
        .setParallelism(p)
        .setPipelining(ppq)
        .setBufferSize((int) CooperativeChannels.intendedTransfer.getBufferSize())
        .build();
  }

  public static XferList readInputFilesFromFile(InputStream stream, String src, String dst) throws Exception {
    //FileInputStream fstream = new FileInputStream(new File(fileName));
    BufferedReader br = new BufferedReader(new InputStreamReader(stream));
    // Find number of lines in the file to create an array of files beforehead
    // rather than using linkedlists which is slow.
    XferList fileEntries = new XferList(src, dst);
    String strLine;
    while ((strLine = br.readLine()) != null) {
      // Print the content on the console
      String[] fileEntry = strLine.split(" ");
      String name = fileEntry[0];
      long size = Long.parseLong(fileEntry[1]);
      fileEntries.add(name, size);
    }
    br.close();
    return fileEntries;
  }

  public static List<Integer> getListOfChannelsOfAChunk(XferList xl) {
    List<Integer> list = new LinkedList<Integer>();
    for (CooperativeModule.ChannelPair channel : xl.channels) {
      list.add(channel.getId());
    }
    return list;
  }
}
