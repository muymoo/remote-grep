package org.uiuc.cs.distributed.grep;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Logger;

public class RemoteGrepApplication {

	private static ExecutorService threadPool;
	private final String logLocation = "/tmp/cs425_momontbowling";
	private static Logger LOGGER;
	private static Handler logFileHandler;
	private static RemoteGrepApplication instance = null;
	private static GrepServer grepServer = new GrepServer();

	private RemoteGrepApplication() {
		threadPool = Executors.newFixedThreadPool(4);
		try {
			logFileHandler = new FileHandler(logLocation + File.separator
					+ "logs" + File.separator + "remotegrepapplication.log");
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		LOGGER = Logger.getLogger("RemoteGrepApplication");
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

		// So we can listen for incoming grep requests.
		startGrepServer();

		LOGGER.info("RemoteGrepApplication - main - starting app");

		String hostname = "";
		try {
			hostname = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e1) {
			LOGGER.warning("RemoteGrepApplication - main- failed to identify host");
		}
		System.out.println("Node " + hostname + " active.");

		// TODO: add configuration for what hostnames, ips, ports the other
		// machine are running on

		InputStreamReader inputStreamReader = new InputStreamReader(System.in);
		BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
		String input = "";
		long start = 0;
		while (true) {
			System.out
					.println("Type 'a' to add node, 'q' to query logs, or 'e' to exit:");
			try {
				input = bufferedReader.readLine();
			} catch (IOException e) {
				LOGGER.warning("RemoteGrepApplication - main- failed to readline from the input");
			}

			if ("a".equals(input.trim())) {

			} else if ("q".equals(input.trim())) {
				start = System.currentTimeMillis();
				app.run();
			} else if ("e".equals(input.trim())) {
				// Close all open client threads
				threadPool.shutdown();

				while (!threadPool.isTerminated()) {
					// wait for all threads to finish
				}

				long end = System.currentTimeMillis();
				System.out.println("All runnables have completed in: "
						+ (end - start) + "ms");

				System.exit(0); // Probably not the best way to stop, but we
								// need to kill the grep server thread somehow
				break;
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
	private static void startGrepServer() {
		grepServer.start();
	}

	/**
	 * Creates four threads to call grep on the 4 different servers.
	 */
	public void run() {
		prompt();
		String grepCommand = readInput();
		for (int i = 0; i < 1; i++) {
			threadPool.execute(new GrepTask("localhost", 4444, grepCommand));
		}
	}

	/**
	 * Prompt user for input
	 */
	public void prompt() {
		System.out.print("Enter grep command>>");
	}

	/**
	 * Reads the users input. TODO: Processing of the grep command could go
	 * here.
	 * 
	 * @return The grep command
	 */
	public String readInput() {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String grepCommand = null;

		try {
			grepCommand = br.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return grepCommand;
	}

}
