package org.uiuc.cs.distributed.grep;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

public class Grep
{
    /**
     * Runs grep on /tmp/LOG_LOCATION/logs/*
     * 
     * @param regex
     *            - Regular expression to search with. This may include flags at the begining.
     * @return The results of the grep command
     */
    public String search(String regex)
    {
        Process process = null;
        String result = "";
        try
        {
            // Use a shell so we can use wildcard (*) for the with ProcessBuilder
            process = new ProcessBuilder("/bin/sh", "-c", "grep " + regex + " " + Application.logLocation + File.separator + "logs/*")
                    .redirectErrorStream(true).start();

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
