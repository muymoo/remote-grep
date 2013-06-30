package org.uiuc.cs.distributed.grep;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * Main entry to the distributed grep program. This should be started on each
 * node you want to query. The default nodes are linux[5-7] whose IP's are hard
 * coded. The default log location is in /tmp/cs425_momontbowling2/.
 * 
 * @author matt
 * @author evan
 */
public class RemoteGrepApplication
{
	public static final String           logLocation = "/tmp/cs425_momontbowling2";
    private static final String 		 LINUX_5 	 = "130.126.112.148";
    private static final String 		 TCP_PORT	 = "4444";
    private static final String 		 UDP_PORT	 = "4445";
    private static Logger                LOGGER;
    private static Handler               logFileHandler;
    private static RemoteGrepApplication instance    = null;
    private GrepServer                   grepServer  = new GrepServer();
    private GroupServer					 groupServer = new GroupServer();
    public ArrayList<GrepTask>           grepTasks;
    public GrepTask                      taskToStopServer;
    private static RemoteGrepApplication app         = RemoteGrepApplication.getInstance();
    private static String[]              servers     = new String[]
                                                     {
            LINUX_5 + ":" + TCP_PORT, "130.126.112.146:4444", "130.126.112.117:4444"
                                                     };
	private static GroupClient groupClient;
    public static List<Node> groupMemebershipList = Collections.synchronizedList(new ArrayList<Node>());

    private String hostaddress = "";
    

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
	 * The main driver function. This function calls sever preparation function,
	 * prompts the user for input, and delegates tasks based on the user's
	 * requests.
	 * 
	 * @param args
	 *            - ignored
	 */
    public static void main(String[] args)
    {

        app.startGrepServer();  // listen for incoming grep requests.
		
        try
        {
            app.hostaddress = InetAddress.getLocalHost().getHostAddress();

            System.out.println("RemoteGrepApplication Server started on: " + app.hostaddress + ":"
                    + app.grepServer.getPort());
            LOGGER.info("RemoteGrepApplication Server started on: " + app.hostaddress + ":" + app.grepServer.getPort());
        }
        catch (UnknownHostException e1)
        {
            LOGGER.warning("RemoteGrepApplication - main- failed to identify host");
        }
		
		// If this is the introducer node, start listening for incoming requests now.
		if(app.hostaddress.equals(LINUX_5))
		{	
			// Add Linux5 as the first memeber 
	        String timestamp =  String.valueOf(System.currentTimeMillis());
	        Node newNode = new Node(timestamp, app.hostaddress, Integer.valueOf(UDP_PORT));
			RemoteGrepApplication.groupMemebershipList.add(newNode);
			
			app.startGroupServer();
			System.out.println("Group Server started");
		}

        InputStreamReader inputStreamReader = new InputStreamReader(System.in);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
        String input = "";

        while (true)
        {
            promptUserForInput();
            
            try
            {
                input = bufferedReader.readLine();
                
                // Add a node to query
                if ( "a".equals(input.trim()) )
                {
                    System.out.println("Enter IP and port (e.g. \"1.2.3.4:4444\"): ");
                    String ipAndPort = bufferedReader.readLine();
                    addTaskForNode(new Node(ipAndPort));
                }
                // Add node to group membership list
                else if("j".equals(input.trim()))
                {
                	joinGroup();
                }
                // Add Default nodes
                else if ( "d".equals(input.trim()) )
                {
                    addDefaultNodes();
                }
                else if ("g".equals(input.trim()))
                {
                	System.out.println(RemoteGrepApplication.groupMemebershipList);
                }
                // Run grep
                else if ( "q".equals(input.trim()) )
                {
                    System.out.print("Enter grep regex>");
                    String regex = bufferedReader.readLine();
                    
                    long start = System.currentTimeMillis();
                    runGrepTasks(regex);
                    joinGrepTasks();
                    long end = System.currentTimeMillis();
                    
                    System.out.println("Total Time: " + (end - start) + "ms");
                }
                // Exit
                else if ( "e".equals(input.trim()) )
                {
                    app.stopGrepServer();
                    app.stopGroupServer();
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

	private static void promptUserForInput() {
		if (RemoteGrepApplication.groupMemebershipList.contains(app.hostaddress)) 
		{
			System.out.println("This node is part of the group list already.");
		} else 
		{
			System.out.println("(j) Join group");
		}
		System.out.println("(g) Current memebership list");
		System.out.println("(a) Add node ((d) adds default nodes)");
		System.out.println("(q) Query logs");
		System.out.println("(e) Exit");
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

    public static void joinGroup()
    {
    	// Send IP to Linux7
    	groupClient = new GroupClient(new Node(LINUX_5 + ":" + UDP_PORT));
    	groupClient.start();
    	try {
			groupClient.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
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

    public void startGroupServer()
    {
    	groupServer.start();
    }
    
    public void stopGroupServer()
    {
    	if(groupServer.isAlive())
    	{
    		groupServer.stopServer();
        	try {		
    			groupServer.join();
    		} catch (InterruptedException e) {
    			e.printStackTrace();
    			LOGGER.warning("Could not join GroupServer thread. Abort ship! Ctrl+C");
    		}
    	}
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
