package didclab.cse.buffalo.utils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DecimalFormat;

import didclab.cse.buffalo.CooperativeChannels;
import didclab.cse.buffalo.CooperativeChannels.Density;
import didclab.cse.buffalo.hysterisis.Entry;
import stork.util.XferList;

public class Utils {
	
	public static String printSize(double random, boolean inByte){
		char sizeUnit = 'b';
		if(inByte)
			sizeUnit = 'B';
		DecimalFormat df = new DecimalFormat("###.##");
		if(random<1024.0)
			return df.format(random)+" " + sizeUnit;
		else if(random<1024.0*1024)
			return df.format(random/1024.0)+" K" + sizeUnit;
		else if(random<1024.0*1024*1024)
			return df.format(random/(1024.0*1024))+" M" + sizeUnit;
		else if (random <(1024*1024*1024*1024.0))
			return df.format(random/(1024*1024.0*1024))+" G" + sizeUnit;
		else 
			return df.format(random/(1024*1024*1024.0*1024))+" T" + sizeUnit;
	}
	
	public static int[] getBestParams(XferList xl){
		Density density = Entry.findDensityOfList(xl.avgFileSize(), CooperativeChannels.intendedTransfer.getBDP());
		xl.density = density;
		double avgFileSize = xl.avgFileSize();
		int fileCountToFillThePipe = (int)Math.round(CooperativeChannels.intendedTransfer.getBDP()/avgFileSize);
		int pLevelToFillPipe = (int)Math.ceil(CooperativeChannels.intendedTransfer.getBDP()/CooperativeChannels.intendedTransfer.getBufferSize());
		int pLevelToFillBuffer = (int)Math.ceil(avgFileSize/CooperativeChannels.intendedTransfer.getBufferSize());
		int cc = Math.max(Math.min(Math.min(fileCountToFillThePipe, xl.count()), CooperativeChannels.intendedTransfer.getMaxConcurrency()), 2);
		int ppq = fileCountToFillThePipe;
		int p =Math.max(Math.min(pLevelToFillPipe, pLevelToFillBuffer) , 1);
		p = avgFileSize > CooperativeChannels.intendedTransfer.getBDP() ? p+1 : p;
		return new int[] {cc,p,ppq,(int)CooperativeChannels.intendedTransfer.getBufferSize()};
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
		  String [] fileEntry = strLine.split(" ");
		  String name = fileEntry[0];
		  long size = Long.parseLong(fileEntry[1]);
		  fileEntries.add(name, size);
		}
		br.close();
		return fileEntries;
	}
}
