package didclab.cse.buffalo;

public class ConfigurationParams {

  public static String MATLAB_DIR = "/Applications/MATLAB_R2014b.app/bin/";
  public static String MATLAB_SCRIPT_DIR = "/Users/earslan/HysterisisBasedMC/matlab";
  public static String INPUT_DIR = "/Users/earslan/HysterisisBasedMC/inputs/activeFiles/";
  public static String OUTPUT_DIR = "/Users/earslan/HysterisisBasedMC/outputs/";
  public static long MAXIMUM_SINGLE_FILE_SIZE = 1024 * 1024 * 1024; // 1GB
  public static String STDOUT_ID = "stdout";
  public static String INFO_LOG_ID = "throughput.log";
  static String HOME_DIR = "/Users/earslan/HysterisisBasedMC/";

  public static void init() {
    MATLAB_SCRIPT_DIR = HOME_DIR + "/matlab";
    INPUT_DIR = HOME_DIR + "/inputs/activeFiles/";
    OUTPUT_DIR = HOME_DIR + "/outputs";
  }
}
