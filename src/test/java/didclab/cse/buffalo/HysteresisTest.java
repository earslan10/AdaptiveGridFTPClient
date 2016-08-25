package didclab.cse.buffalo;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by earslan on 8/24/16.
 */
public class HysteresisTest {

    @Test
    public void testPythonOptimizer() {
        ProcessBuilder pb = new ProcessBuilder("python", "python/optimizer.py",
                "-f", "chunk_0.txt",
                "-p", "" + 2,
                "-c", "" + 2,
                "-q", ""+ 2,
                "-t", "" + 1210.10);
        System.out.println("Output:" + pb.command());
        Process p = null;
        try {
            p = pb.start();
        } catch (IOException e) {
            e.printStackTrace();
        }


        BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
        String output = null;
        try {
            output = in.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Output:" + output);
        String []values = output.replaceAll("\\[", "").replaceAll("\\]", "").split("\\s+");
        for (int i = 0; i < values.length; i++) {
            System.out.println("Value:" + Double.parseDouble(values[i]));
        }

        in = new BufferedReader(new InputStreamReader(p.getErrorStream()));
        try {
            while ((output = in.readLine()) != null){
                System.out.println(output);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
