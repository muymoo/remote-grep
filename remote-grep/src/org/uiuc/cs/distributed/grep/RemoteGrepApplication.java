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

public class RemoteGrepApplication {

	public static final String logLocation = "/tmp/cs425_momontbowling";
	
	private static Logger LOGGER;
	private static Handler logFileHandler;
	private static RemoteGrepApplication instance = null;
	private GrepServer grepServer = new GrepServer();
	public ArrayList<GrepTask> grepTasks;
	public GrepTask greptask1;
    private static RemoteGrepApplication app = RemoteGrepApplication.getInstance();

	private RemoteGrepApplication() {
		String logFileLocation = logLocation + File.separator
				+ "logs" + File.separator + "remotegrepapplication.log";
		
		try {
			logFileHandler = new FileHandler(logFileLocation);
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		LOGGER = Logger.getLogger("RemoteGrepApplication");
		LOGGER.setUseParentHandlers(false);
		logFileHandler.setFormatter(new SimpleFormatter());
		logFileHandler.setLevel(Level.INFO);
		LOGGER.addHandler(logFileHandler);
		this.grepTasks = new ArrayList<GrepTask>();
	}

	public static RemoteGrepApplication getInstance() {
		if (instance == null) {
			instance = new RemoteGrepApplication();
		}
		return instance;
	}

	/**
	 * the main driver function
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		
		app.startGrepServer();  // listen for incoming grep requests.

		
		String hostaddress = "";
		try {
			hostaddress = InetAddress.getLocalHost().getHostAddress();
			
			System.out.println("RemoteGrepApplication Server started on: "+hostaddress+":"+app.grepServer.getPort());
			LOGGER.info("RemoteGrepApplication Server started on: "+hostaddress+":"+app.grepServer.getPort());
		} catch (UnknownHostException e1) {
			LOGGER.warning("RemoteGrepApplication - main- failed to identify host");
		}

		InputStreamReader inputStreamReader = new InputStreamReader(System.in);
		BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
		String input = "";
		
		while (true) {
			System.out.println("Type 'a' to add node, 'q' to query logs, or 'e' to exit:");
			long start;
			try {
				input = bufferedReader.readLine();
				if ("a".equals(input.trim())) {
					System.out.println("Enter IP and port (e.g. \"1.2.3.4:4444\"): ");
					String ipAndPort = bufferedReader.readLine();
					// TODO: add isValid() check on input
					addTaskForNode(new Node(ipAndPort));
				} else if ("q".equals(input.trim())) {
					start = System.currentTimeMillis();
					System.out.print("Enter grep regex>");
					String regex = bufferedReader.readLine();
					runGrepTasks(regex);
					joinGrepTasks();
					long end = System.currentTimeMillis();
					System.out.println("Total Time: "+(end-start)+"ms");
				} else if ("e".equals(input.trim())) {
					app.stopGrepServer();
					break;
				}
			} catch (IOException e) {
				LOGGER.warning("RemoteGrepApplication - main- failed to readline from the input");
			}
		}
		try {
			bufferedReader.close();
		} catch (IOException e) {
			LOGGER.warning("RemoteGrepApplication - main- failed to close bufferedreader");
		}
		try {
			inputStreamReader.close();
		} catch (IOException e) {
			LOGGER.warning("RemoteGrepApplication - main- failed to close inputstreamreader");
		}
	}

    /**
     * 
     */
    public static void joinGrepTasks()
    {
        for(GrepTask grepTask : app.grepTasks) {
        	try {
        		grepTask.join();
        	} catch (InterruptedException e) {
        		// TODO Auto-generated catch block
        		e.printStackTrace();
        	}
        	System.out.println("From node: "+grepTask.getNode().toString());
        	System.out.print(grepTask.getResult());
        }
    }

    /**
     * @param grepCommand
     */
    public static void runGrepTasks(String regex)
    {
        for(GrepTask grepTask : app.grepTasks) {
        	grepTask.setRegex(regex);
        	grepTask.start();
        }
    }

	public static void addTaskForNode(Node node) {
        app.grepTasks.add(new GrepTask(node));
	}
	
	/**
	 * Listens on port 4444 for incoming grep requests
	 */
	private void startGrepServer() {
		grepServer.start();
	}
	
	private void stopGrepServer() {
		greptask1 = new GrepTask(new Node("localhost", 4444));
		greptask1.setRegex("<QUIT>");
		greptask1.start();
		try {
			greptask1.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			grepServer.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			LOGGER.warning("RemoteGrepApplication - stopGrepServer - interrupted while joining processing threads.");
		}
	}

}
