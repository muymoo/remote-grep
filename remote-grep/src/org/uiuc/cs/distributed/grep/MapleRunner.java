package org.uiuc.cs.distributed.grep;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * Listens on each node for an incoming maple request and then do the deed.
 * 
 * @author matt
 * 
 */
public class MapleRunner extends Thread {
	private final int MAPLE_EXE_SDFS_KEY = 1;
	private final int INTERMEDIATE_PREFIX = 2;
	private final int SDFS_SOURCE_FILE = 3;

	public MapleRunner() {
		super("MapleRunner");
	}

	@Override
	public void run() {
		waitForIncomingMapleCommand();
		// TODO: Join all maple threads
		// TODO: Send response back to Master
	}

	/**
	 * Block until a command from the master has been received
	 */
	private void waitForIncomingMapleCommand() {
		Socket mapleSocket;
		try {
			// Connect to leader node (the one sending maple commands)
			mapleSocket = new Socket(Application.getInstance().group
					.getLeader().getIP(), Application.TCP_MAPLE_PORT);
			// Setup input steam to listen with
			BufferedReader in = new BufferedReader(new InputStreamReader(
					mapleSocket.getInputStream()));
			String mapleCommandFromServer;
			while ((mapleCommandFromServer = in.readLine()) != null) {
				System.out.println("Maple Command received: "
						+ mapleCommandFromServer);
				startMapleTask(mapleCommandFromServer);

			}
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Start a new maple task for this assignment from master.
	 * 
	 * @param mapleCommandFromServer
	 *            Message from the server in the form of
	 *            MAPLE:MAPLE_EXE_SDFS_KEY:INT_PREFIX:SOURCE_FILE_SDFS_KEY
	 */
	private void startMapleTask(String mapleCommandFromServer) {
		String[] command = parseMapleCommand(mapleCommandFromServer);
		new MapleTask(command[MAPLE_EXE_SDFS_KEY],
				command[INTERMEDIATE_PREFIX], command[SDFS_SOURCE_FILE])
				.start();
	}

	/**
	 * Split the maple command into parts
	 * 
	 * @param mapleCommandFromServer
	 *            ":" delimited message from server
	 * @return Command split on ":"
	 */
	private String[] parseMapleCommand(String mapleCommandFromServer) {
		return mapleCommandFromServer.split(":");
	}
}
