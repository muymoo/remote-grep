package org.uiuc.cs.distributed.grep.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;

import org.uiuc.cs.distributed.grep.RemoteGrepApplication;

/**
 * This class helps create sample log files with dummy data of a specified size.
 * 
 * @author evan
 */
public class TestLogs
{
    private static final String errorMessages[] =
                                                {
            "To the dark, dark seas comes the only whale; Watching ships go\n",
            "It's a Casio on a platic beach; It's a Casio on plastic beach;\n",
            "It's a Sytrofoam deep sea landfill; It's a Styrofoam deep sea \n",
                                                };

    /**
     * To use:</br>
     * 
     * <pre>
     * java -cp testclasses org.uiuc.cs.distributed.grep.util.TestLogs 6 100000
     * </pre>
     * 
     * Which will create a log file machine.6.log with 100000 lines of sample log messages.
     * 
     * @param args - Optional inputs of machine_index number_of_lines_in_file
     */
    public static void main(String[] args)
    {
        if ( args.length > 1 )
        {
            createLogFile(Integer.parseInt(args[0]), Integer.parseInt(args[1]));
        }
        else
        {
            createLogFile(5, 10000);
        }
    }

    /*

     */
    /**
     * Creates a log file named "machine.i.log" with i as the input, and is stored at the given log location
     * all the dates/times within the logs start at March 01, 2013 CST
     * NOTES:
     * <ul>
     * <li>Frequent "INFO" slog lines = 80%</li>
     * <li>Infrequent "WARNING" log lines = 19%</li>
     * <li>Rare "SEVERE" log lines = 1%</li>
     * </ul>
     * 
     * @param machineNumber - Number of linux machine this log file will be created on (ex. for linux6, this would be 6 and would create a machine.6.log file)
     * @param numberOfLines - Number of lines for log file to have [100000=10MB, 1000000=100MB, 10000000=1GB]
     */
    public static void createLogFile(int machineNumber, int numberOfLines)
    {
        try
        {
            FileWriter fileWriter = null;
            BufferedWriter bufferedWriter = null;

            File dummyLogFile = new File("machine."
                    + machineNumber + ".log");

            fileWriter = new FileWriter(dummyLogFile.getAbsoluteFile());

            bufferedWriter = new BufferedWriter(fileWriter);

            String logType = "";
            Date logDate = new Date();
            long time = 1362117600; // 1362114000 Mar 01,2013 0:0:0 CST

            System.out.println("Time: " + logDate.getTime());

            for (int lineIndex = 0; lineIndex < numberOfLines; lineIndex++)
            {
                // adjust the logType and the time based on the line number
                if ( lineIndex < numberOfLines / 100 )
                { // Rare
                    logType = "SEVERE "; // padding necessary for consistent line length
                }
                else if ( lineIndex >= numberOfLines / 100 && lineIndex < (numberOfLines / 5) )
                { // Infrequent
                    logType = "WARNING";
                }
                else if ( lineIndex >= (numberOfLines / 5) )
                { // Frequent
                    logType = "INFO   "; // padding necessary for consistent line length
                }
                logDate.setTime((time * 1000) + (lineIndex * 60000));

                // select a random error message
                int min = 0;
                int max = errorMessages.length;
                int errorMessageIndex = min + (int) (Math.random() * (max - min));
                String line = logType + " " + logDate.toString() + "-" + errorMessages[errorMessageIndex];
                bufferedWriter.write(line);
            }

            // Extra lines for testing
            bufferedWriter.write("There should be 2 of me.\n");
            bufferedWriter.write("There should be 2 of me.\n");

            bufferedWriter.close();
            fileWriter.close();
        }
        catch (IOException e1)
        {
            e1.printStackTrace();
        }
    }

}
