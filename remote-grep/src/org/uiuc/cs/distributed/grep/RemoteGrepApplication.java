package org.uiuc.cs.distributed.grep;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * Main entry to the distributed grep program. This should be started on each node you want to query. The default nodes are linux[5-7] whose IP's are hard
 * coded. The default log location is in /tmp/cs425_momontbowling2/.
 * 
 * @author matt
 * @author evan
 */
public class RemoteGrepApplication
{

    public static final String           logLocation = "/tmp/cs425_momontbowling2";

    private static Logger                LOGGER;
    private static Handler               logFileHandler;
    private static RemoteGrepApplication instance    = null;
    private GrepServer                   grepServer  = new GrepServer();
    public ArrayList<GrepTask>           grepTasks;
    public GrepTask                      taskToStopServer;
    private static RemoteGrepApplication app         = RemoteGrepApplication.getInstance();
    private static String[]              servers     = new String[]
                                                     {
            "130.126.112.148:4444", "130.126.112.146:4444", "130.126.112.117:4444"
                                                     };

    private RemoteGrepApplication()
    {
        String logFileLocation = logLocation + File.separator + "logs" + File.separator + "remotegrepapplication.log";

        try
        {
            logFileHandler = new FileHandler(logFileLocation);
        }
        catch (SecurityException e)
        {
            e.printStackTrace();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        LOGGER = Logger.getLogger("RemoteGrepApplication");
        LOGGER.setUseParentHandlers(false);
        logFileHandler.setFormatter(new SimpleFormatter());
        logFileHandler.setLevel(Level.INFO);
        LOGGER.addHandler(logFileHandler);
        this.grepTasks = new ArrayList<GrepTask>();
    }

    /**
     * We are using a singleton pattern for our application.
     * 
     * @return - Grep application instance
     */
    public static RemoteGrepApplication getInstance()
    {
        if ( instance == null )
        {
            instance = new RemoteGrepApplication();
        }
        return instance;
    }

    /**
     * The main driver function. This function calls sever preparation function, prompts the user for input, and delegates tasks based on the user's requests.
     * 
     * @param args - ignored
     */
    public static void main(String[] args)
    {

        app.startGrepServer();  // listen for incoming grep requests.

        String hostaddress = "";
        try
        {
            hostaddress = InetAddress.getLocalHost().getHostAddress();

            System.out.println("RemoteGrepApplication Server started on: " + hostaddress + ":"
                    + app.grepServer.getPort());
            LOGGER.info("RemoteGrepApplication Server started on: " + hostaddress + ":" + app.grepServer.getPort());
        }
        catch (UnknownHostException e1)
        {
            LOGGER.warning("RemoteGrepApplication - main- failed to identify host");
        }

        InputStreamReader inputStreamReader = new InputStreamReader(System.in);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        String input = "";

        while (true)
        {
            System.out.println("Type 'a' to add node ('d' adds default nodes), 'q' to query logs, or 'e' to exit:");
            long start;
            try
            {
                input = bufferedReader.readLine();
                
                // Add a node
                if ( "a".equals(input.trim()) )
                {
                    System.out.println("Enter IP and port (e.g. \"1.2.3.4:4444\"): ");
                    String ipAndPort = bufferedReader.readLine();
                    addTaskForNode(new Node(ipAndPort));
                }
                // Add Default nodes
                else if ( "d".equals(input.trim()) )
                {
                    addDefaultNodes();
                }
                // Run grep
                else if ( "q".equals(input.trim()) )
                {
                    start = System.currentTimeMillis();
                    System.out.print("Enter grep regex>");
                    String regex = bufferedReader.readLine();
                    runGrepTasks(regex);
                    joinGrepTasks();
                    long end = System.currentTimeMillis();
                    System.out.println("Total Time: " + (end - start) + "ms");
                }
                // Exit
                else if ( "e".equals(input.trim()) )
                {
                    app.stopGrepServer();
                    break;
                }
            }
            catch (IOException e)
            {
                LOGGER.warning("RemoteGrepApplication - main- failed to readline from the input");
            }
        }
        try
        {
            bufferedReader.close();
        }
        catch (IOException e)
        {
            LOGGER.warning("RemoteGrepApplication - main- failed to close bufferedreader");
        }
        try
        {
            inputStreamReader.close();
        }
        catch (IOException e)
        {
            LOGGER.warning("RemoteGrepApplication - main- failed to close inputstreamreader");
        }
    }

    /**
     * Creates a task for each of the default linux nodes (linux[5-7])
     */
    public static void addDefaultNodes()
    {
        for (String server : servers)
        {
            addTaskForNode(new Node(server));
        }
    }

    /**
     * Waits for all of the grep tasks to complete and prints their results to the console
     */
    public static void joinGrepTasks()
    {
        for (GrepTask grepTask : app.grepTasks)
        {
            try
            {
                grepTask.join();
            }
            catch (InterruptedException e)
            {
                e.printStackTrace();
            }
            System.out.println("From node: " + grepTask.getNode().toString());
            System.out.print(grepTask.getResult());
        }
        app.grepTasks.clear();
        System.out.println("Grep Successful: Removed all nodes. Please re-add nodes you would like to search again.");
    }

    /**
     * So we can run all of the setup grep tasks for each server.
     * 
     * @param regex - grep command to search for. This may include flags at the beginning. (Ex. -rni severe)
     */
    public static void runGrepTasks(String regex)
    {
        for (GrepTask grepTask : app.grepTasks)
        {
            grepTask.setRegex(regex);
            grepTask.start();
        }
    }

    /**
     * We need to store which servers to run grep tasks on.
     * 
     * @param node - Server to create a grep task for
     */
    public static void addTaskForNode(Node node)
    {
        app.grepTasks.add(new GrepTask(node));
    }

    /**
     * Listens on port default port for incoming grep requests
     */
    public void startGrepServer()
    {
        grepServer.start();
    }

    /**
     * Sends quit message to the grep server instance and waits for the grep sever thread to complete.
     */
    public void stopGrepServer()
    {
        taskToStopServer = new GrepTask(new Node("localhost", 4444));
        taskToStopServer.setRegex("<QUIT>");
        taskToStopServer.start();
        try
        {
            taskToStopServer.join();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
            LOGGER.warning("RemoteGrepApplication - stopGrepServer - interrupted while joining task thread.");
        }
        try
        {
            grepServer.join();
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
            LOGGER.warning("RemoteGrepApplication - stopGrepServer - interrupted while joining processing threads.");
        }
    }
}
