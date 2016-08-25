package didclab.cse.buffalo;

import java.io.File;

public class ConfigurationParams {

  public static String MATLAB_DIR = "/Applications/MATLAB_R2014b.app/bin/";
  public static String MATLAB_SCRIPT_DIR = "/Users/earslan/HysterisisBasedMC/matlab";
  public static String INPUT_DIR = "/Users/earslan/HysterisisBasedMC/historical_data/activeFiles/";
  public static String OUTPUT_DIR = "/Users/earslan/HysterisisBasedMC/outputs/";
  public static long MAXIMUM_SINGLE_FILE_SIZE = 1024 * 1024 * 1024; // 1GB
  public static String STDOUT_ID = "stdout";
  static String INFO_LOG_ID = "throughput.log";
  static String proxyFilePath = null;

  static void init() {
    String home_dir_path = new File("").getAbsolutePath();
    MATLAB_SCRIPT_DIR = home_dir_path + "/matlab";
    INPUT_DIR = home_dir_path + "/inputs/activeFiles/";
    OUTPUT_DIR = home_dir_path + "/outputs";
  }
}
