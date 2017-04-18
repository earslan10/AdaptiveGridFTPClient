package client.utils;

import client.AdaptiveGridFTPClient;
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

  public static TunableParameters getBestParams(XferList xl, int maximumChunks) {
    Density density = findDensityOfFile((long) xl.avgFileSize(), AdaptiveGridFTPClient.transferTask.getBandwidth(),
        maximumChunks);
    xl.density = density;
    double avgFileSize = xl.avgFileSize();
    int fileCountToFillThePipe = (int) Math.round(AdaptiveGridFTPClient.transferTask.getBDP() / avgFileSize);
    int pLevelToFillPipe = (int) Math.ceil(AdaptiveGridFTPClient.transferTask.getBDP() / AdaptiveGridFTPClient.transferTask.getBufferSize());
    int pLevelToFillBuffer = (int) Math.ceil(avgFileSize / AdaptiveGridFTPClient.transferTask.getBufferSize());
    int cc = Math.min(Math.min(Math.max(fileCountToFillThePipe, 3), xl.count()), AdaptiveGridFTPClient.transferTask.getMaxConcurrency());
    int ppq = fileCountToFillThePipe;
    int p = Math.max(Math.min(pLevelToFillPipe, pLevelToFillBuffer), 1);
    p = avgFileSize > AdaptiveGridFTPClient.transferTask.getBDP() ? p : p;
    return new TunableParameters.Builder()
        .setConcurrency(cc)
        .setParallelism(p)
        .setPipelining(ppq)
        .setBufferSize((int) AdaptiveGridFTPClient.transferTask.getBufferSize())
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
      String path = fileEntry[0];
      long size = Long.parseLong(fileEntry[1]);
      fileEntries.add(path, size);
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

  public static Density findDensityOfFile(long fileSize, double bandwidth, int maximumChunks) {
    double bandwidthInMB = bandwidth / 8.0;
    if (fileSize <= bandwidthInMB / 20) {
      return Density.SMALL;
    } else if (maximumChunks > 3 && fileSize > bandwidthInMB * 2) {
      return Density.HUGE;
    } else if (maximumChunks > 2 && fileSize <= bandwidthInMB / 5) {
      return Density.MEDIUM;
    } else {
      return Density.LARGE;
    }
  }


  // Ordering in this enum is important and has to stay as this for Utils.findDensityOfFile to perform as expected
  public enum Density {
    SMALL, LARGE, MEDIUM, HUGE
  }
}
