package didclab.cse.buffalo;


import stork.module.CooperativeModule.GridFTPTransfer;

public class ConfigurationParams {
	public static String MATLAB_DIR = "/home/earslan/matlab15/bin/";
	public static String MATLAB_SCRIPT_DIR = "/home/earslan/Dropbox/Log_Based_Throughput_Optimization/CooperativeChannels/matlab_scripts";
	public static String INPUT_DIR = "/home/earslan/Dropbox/Log_Based_Throughput_Optimization/inputs/";
	public static String OUTPUT_DIR = "/home/earslan/Dropbox/Log_Based_Throughput_Optimization/CooperativeChannels/outputs";
	
	
	public static String SourceServer ,DestinationServer ,ProxyFile;
	public static int MaxReadBuffer, MaxWriteBuffer;
	public static double RTT = 65 ,BANDWIDTH=10, BDP = 65;
	public static int BufferSize =  32*1024*1024;  //32M for XSEDE
	
	static final int DoStriping = 1;
	static final int DontStriping = 0;
	
	static long init;
	static GridFTPTransfer GridFTPClient = null;
	static double totalTransferTime = 0;
	
	//Log Based approach parameters
	public static String TESTBED;
	static int MAX_CONCURRENCY = 20;
	static boolean USE_HISTORY = true;	
	
	public static String STDOUT_ID = "stdout";
	public static String INFO_LOG_ID = "throughput.log";
}
