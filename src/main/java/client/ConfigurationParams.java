package client;

import java.io.File;

public class ConfigurationParams {

  public static String INPUT_DIR = "/Users/earslan/HARP/historical_data/activeFiles/";
  public static String OUTPUT_DIR = "/Users/earslan/HARP/outputs/";
  public static long MAXIMUM_SINGLE_FILE_SIZE = 1024 * 1024 * 1024; // 1GB
  public static String STDOUT_ID = "stdout";
  public static double cc_rate = 0.7;
  public static double p_rate = 0.7;
  public static double ppq_rate = 0.99;
  static String INFO_LOG_ID = "throughput.log";

  static void init() {
    String home_dir_path = new File("").getAbsolutePath();
    INPUT_DIR = home_dir_path + "/historical_data/activeFiles/";
    OUTPUT_DIR = home_dir_path + "/outputs";
  }
}
