package org.uiuc.cs.distributed.grep;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Logger;

public class RemoteGrepApplication {

	public static final String logLocation = "/tmp/cs425_momontbowling";
	
	private static Logger LOGGER;
	private static Handler logFileHandler;
	private static RemoteGrepApplication instance = null;
	private GrepServer grepServer = new GrepServer();
	public ArrayList<GrepTask> grepTasks;
	public GrepTask greptask1;

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
		for(Handler handler : LOGGER.getHandlers()) {
		    LOGGER.removeHandler(handler);
		}
		LOGGER.addHandler(logFileHandler);
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
		RemoteGrepApplication app = RemoteGrepApplication.getInstance();
		
		app.startGrepServer();  // listen for incoming grep requests.

		String hostname = "";
		try {
			hostname = InetAddress.getLocalHost().getHostName();
			String hostaddress = InetAddress.getLocalHost().getHostAddress();
			
			System.out.println("RemoteGrepApplication - main - hostname: "+hostname);
			System.out.println("RemoteGrepApplicaiton - main - hostaddress: "+hostaddress);
		} catch (UnknownHostException e1) {
			LOGGER.warning("RemoteGrepApplication - main- failed to identify host");
		}

		InputStreamReader inputStreamReader = new InputStreamReader(System.in);
		BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
		String input = "";
		
		while (true) {
			System.out.println("Type 'a' to add node, 'q1|q2' to query logs, or 'e' to exit:");
			long start;
			try {
				input = bufferedReader.readLine();
				if ("a".equals(input.trim())) {
					System.out.println("add logic");
					System.out.println("Enter IP and port (e.g. \"1.2.3.4:4444\"): ");
					
				} else if ("q1".equals(input.trim())) {
					app.greptask1 = new GrepTask(new Node("localhost", 4444));
					System.out.print("Enter grep command>");
					String grepCommand = bufferedReader.readLine();
					app.greptask1.setRegex(grepCommand);
					app.greptask1.start();
					try {
						app.greptask1.join();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					System.out.println(app.greptask1.getResult());
				} else if("q2".equals(input.trim())) {
					app.greptask1 = new GrepTask(new Node("130.126.112.146", 4444));
					System.out.print("Enter grep command>");
					String grepCommand = bufferedReader.readLine();
					app.greptask1.setRegex(grepCommand);
					app.greptask1.start();
					try {
						app.greptask1.join();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					System.out.println(app.greptask1.getResult());
				} else if ("e".equals(input.trim())) {
					start = System.currentTimeMillis();
					long end = System.currentTimeMillis();
					
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
