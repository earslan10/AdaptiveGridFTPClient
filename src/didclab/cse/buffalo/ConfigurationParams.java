package didclab.cse.buffalo;


import stork.module.CooperativeModule.GridFTPTransfer;

public class ConfigurationParams {
	
	public static String HOME_DIR = "/home/earslan/Log_Based_Throughput_Optimization/CooperativeChannels";
	public static String MATLAB_DIR = "/home/earslan/matlab/bin/";
	public static String MATLAB_SCRIPT_DIR = "/home/earslan/Log_Based_Throughput_Optimization/matlab_scripts";
	public static String INPUT_DIR = "/home/earslan/Log_Based_Throughput_Optimization/inputs/";
	public static String OUTPUT_DIR = "/home/earslan/Log_Based_Throughput_Optimization/CooperativeChannels/outputs";
	
	
	
	static String ProxyFile;
	static GridFTPTransfer GridFTPClient = null;
	static double totalTransferTime = 0;
	
	//Log Based approach parameters
	static boolean USE_HISTORY = false;	
	
	public static String STDOUT_ID = "stdout";
	public static String INFO_LOG_ID = "throughput.log";
	
	public static void init(){
		 MATLAB_SCRIPT_DIR = HOME_DIR + "/matlab";
		INPUT_DIR =  HOME_DIR + "/inputs/";
		OUTPUT_DIR =  HOME_DIR + "/outputs";
	}
}
