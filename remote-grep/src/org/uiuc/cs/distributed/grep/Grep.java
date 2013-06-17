package org.uiuc.cs.distributed.grep;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

public class Grep
{

    public Grep()
    {
        // createDummyLogFiles();
    }

    /**
     * Creates dummy log files to test with
     */
    /*
     * private void createDummyLogFiles() {
     * String logLine = "14:53 [ERROR] Cannot read machine code.";
     * for (int i = 0; i < 5; i++) {
     * File dummyLogFile = new File("/tmp/cs425_momontbowling/machine." + i + ".log");
     * FileWriter fileWriter = null;
     * try {
     * fileWriter = new FileWriter(dummyLogFile.getAbsoluteFile());
     * } catch (IOException e1) {
     * e1.printStackTrace();
     * }
     * BufferedWriter writer = new BufferedWriter(fileWriter);
     * try {
     * writer.write(logLine);
     * } catch (IOException e) {
     * e.printStackTrace();
     * }
     * try {
     * writer.close();
     * } catch (IOException e) {
     * e.printStackTrace();
     * }
     * try {
     * fileWriter.close();
     * } catch (IOException e) {
     * e.printStackTrace();
     * }
     * }
     * }
     */

    /**
     * Runs grep on /tmp/machine.1.log
     * 
     * @param regex
     *            - Regular expression to search with
     * @return The results of the grep command
     */
    public String search(String regex)
    {
        Process process = null;
        String result = "";
        try
        {
            process = new ProcessBuilder("grep", "-rni", regex, "/tmp/cs425_momontbowling2/machine*").start();


            BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line = "";
            StringBuilder builder = new StringBuilder();
            while ((line = br.readLine()) != null)
            {
                builder.append(line);
                builder.append("\n");
            }
            result = builder.toString();
            process.waitFor();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        return result;
    }
}
