package didclab.cse.buffalo.log;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Map;


public class LogManager {

	private static Map<String, BufferedWriter> logFiles = new HashMap<String, BufferedWriter>();
	
	static BufferedWriter Throughput_Log;
	static BufferedWriter TransferDetail_Log;
	
	public static BufferedWriter createLogFile(String fileID){
		Writer writer = null;
		try{
			 if (fileID.compareTo("stdout") == 0)
				 writer =  new OutputStreamWriter(System.out);
		    else
		    	 writer =  new FileWriter(fileID, false);
			BufferedWriter bw = new BufferedWriter(writer);
			logFiles.put(fileID, bw);
			return bw;
		}
		catch(Exception e){
			e.printStackTrace();
			return null;
		}
	}
	
	public static BufferedWriter getLogFile(String fileID){
		if(logFiles.containsKey(fileID))
			return logFiles.get(fileID);
		return null;
	}
	
	public static boolean writeToLog(String message, String... fileID){
		try {
			for (String out: fileID){
				BufferedWriter bw = logFiles.get(out);
				bw.append(message);
				bw.append("\n");
				bw.flush();
			}
			return true;
		} 
		catch (Exception e){
			e.printStackTrace();
			return false;
		}
		
	}
	
	public static void close(){
		try {
			for (BufferedWriter bw: logFiles.values()){
				bw.close();
			}
		} 
		catch (Exception e){
			e.printStackTrace();
		}
		
	}
	
}
