package org.uiuc.cs.distributed.grep;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * The grep server listens for regular expressions (regex) coming into port 4444
 * and returns the results of the grep command using the given regex on the
 * machine.1.log file.
 * 
 * @author matt
 * @author evan
 * 
 */
public class GrepServer extends Thread {
	private static Logger LOGGER;
	private static Handler logFileHandler;
	
	private ServerSocket serverSocket = null;
	private int serverPort = 4444;
	private boolean listening = true;
	private boolean foundPort;


	public GrepServer() {
		super("GrepServerThread");
		String logFileLocation = RemoteGrepApplication.logLocation + File.separator
				+ "logs" + File.separator + "grepserver.log";
		
		try {
			logFileHandler = new FileHandler(logFileLocation);
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		LOGGER = Logger.getLogger("GrepServer");
		LOGGER.setUseParentHandlers(false);
		logFileHandler.setFormatter(new SimpleFormatter());
		logFileHandler.setLevel(Level.INFO);
		LOGGER.addHandler(logFileHandler);
		
		this.foundPort = false;
	}
	
	// TODO: introduce thread wait for the this.foundPort condition
	//       be sure it doesn't introduce potential deadlock
	public synchronized int getPort() {
		return this.serverPort;
	}

	public void run() {
		try {
			int numTriesLeft = 1000;
			while(!this.foundPort && numTriesLeft > 0) {
				try {
					this.serverSocket = new ServerSocket(serverPort);
					this.foundPort = true;
				} catch (IOException e) {
					this.serverPort++; // try the next port number
					numTriesLeft--;
				}
			}
			if(!this.foundPort) {
				LOGGER.severe("GrepServer - run - Could not listen on port: "+serverPort);
				System.exit(-1);
			}
			LOGGER.info("GrepServer - run - Server started on socket: "+serverPort);
			
			
			while(listening) {
				Socket clientSocket = serverSocket.accept();
				LOGGER.info("GrepServer - run - accepted connection from: "+clientSocket.getInetAddress()+":"+clientSocket.getPort());
				
				// Setup our input/output streams
				PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
				BufferedReader in = new BufferedReader(new InputStreamReader(
						clientSocket.getInputStream()));
	
				String clientInput, clientOutput;
				Grep grep = new Grep();
				
				// Loop until grep doesn't return any more results
				while((clientInput = in.readLine()) != null) {
					LOGGER.info("GrepServer - run - clientInput: "+clientInput);
					if(clientInput.equals("<QUIT>"))
					{
						listening = false;
						out.println("Shutting Down");
						break;
					}
					clientOutput = grep.search(clientInput); // run grep
					LOGGER.info("GrepServer - run - clientOutput: "+clientOutput);
					out.println(clientOutput); // Send results back to client
				}
				
				out.close();
				in.close();
				clientSocket.close();
				
			}

			serverSocket.close();
			LOGGER.info("GrepServer - run - socket closed, shutting down server.");
		} catch (IOException e) {
			LOGGER.info("GrepServer - run - IOException: "+e.getMessage()+" stack trace: "+e.getStackTrace().toString());
		}
	}
}
