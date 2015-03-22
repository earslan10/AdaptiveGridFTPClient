package didclab.cse.buffalo.hysterisis;

import didclab.cse.buffalo.ConfigurationParams;
import didclab.cse.buffalo.log.LogManager;
import matlabcontrol.MatlabProxy;
import matlabcontrol.MatlabProxyFactory;
import matlabcontrol.MatlabProxyFactoryOptions;

public class ModellingAndOptimization {
	
	
	private MatlabProxy proxy  = null;
	
	public ModellingAndOptimization() {
		// TODO Auto-generated constructor stub
	}
	
	public boolean initializeMatlab(){
		MatlabProxyFactoryOptions options = new MatlabProxyFactoryOptions.Builder()
        .setUsePreviouslyControlledSession(true)
        .setHidden(true)
        .setMatlabLocation(ConfigurationParams.MATLAB_DIR+"/matlab").build();
    	MatlabProxyFactory factory = new MatlabProxyFactory(options);
    	
    	try {
			proxy = factory.getProxy();
			return true;
    	}
    	catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
    	
	}
	
	public Object [] polyFitbyMatlab(int folderID, int logFilesCount, double throughput, int[] maxParams, int []sampleParams){
		Object[] results = null;
    	try {
    		
			proxy.eval("cd "+ConfigurationParams.MATLAB_SCRIPT_DIR);
			String command = "main("+folderID+","+throughput+","+(logFilesCount-1)+",["+sampleParams[0]+","
									+sampleParams[1]+","+sampleParams[2]+"]"+",["+maxParams[0]+","+
									maxParams[1]+","+maxParams[2]+"]"+", '"+ConfigurationParams.OUTPUT_DIR+"')";
			LogManager.writeToLog("\t"+command+"\n", ConfigurationParams.INFO_LOG_ID, ConfigurationParams.STDOUT_ID);
			results = proxy.returningEval(command,2);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	proxy.disconnect();
    	return results;
	}
}
