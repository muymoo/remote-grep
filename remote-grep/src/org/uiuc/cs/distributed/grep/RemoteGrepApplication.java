package org.uiuc.cs.distributed.grep;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
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
 * Main entry to the distributed grep and group membership program. This should be started on each
 * node you want to query. The default nodes are linux[5-7] whose IP's are hard
 * coded. The default log location is in /tmp/cs425_momontbowling3/logs.
 * 
 * @author matt
 * @author evan
 */
public class RemoteGrepApplication {
	public static String logLocation = "";
	public static final int TCP_PORT = 4444;
	public static final int UDP_PORT = 4445;
	public static final int UDP_MC_PORT = 4446;
	public static final int UDP_FD_PORT = 4447; // port for failure detection
	public static final String UDP_MC_GROUP = "228.5.6.7";
	public static final int timeBoundedFailureInMilli = 5000;
	public static Logger LOGGER;

	private static Handler logFileHandler;
	private static RemoteGrepApplication instance = null;
	private GrepServer grepServer;
	private GroupServer groupServer;
	private static GroupClient groupClient;
	private FailureDetectorServer failureDetectorServer;
	private FailureDetectorClient failureDetectorClient;
	public ArrayList<GrepTask> grepTasks;
	public GrepTask taskToStopServer;
	private static final String LINUX_5 = "130.126.112.148";
	private static String[] servers = new String[] { LINUX_5 + ":" + TCP_PORT,
			"130.126.112.146:4444", "130.126.112.117:4444" };

	public static List<Node> groupMembershipList = Collections
			.synchronizedList(new ArrayList<Node>());
	private String hostaddress = "";

	/**
	 * Main server. The log location is where the server logs will be stored as
	 * well as where grep will search.
	 * 
	 * @param newLogLocation
	 *            Location to store logs.
	 */
	private RemoteGrepApplication(String newLogLocation) {
		String hostname = "linux5";
		try {
			hostname = java.net.InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e1) {
			// nothing to do in this case
		}
		this.logLocation = newLogLocation;

		String logFileLocation = this.logLocation + File.separator + "logs"
				+ File.separator + "remotegrepapplication."
				+ hostname.charAt(5) + ".log";

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

	/**
	 * We are using a singleton pattern for our application.
	 * 
	 * @return - Grep application instance
	 */
	public static RemoteGrepApplication getInstance(String newLogLocation) {
		if (instance == null) {
			instance = new RemoteGrepApplication(newLogLocation);
		}
		return instance;
	}

	/**
	 * Configures/starts the failure detection threads and sets the intentional
	 * message failure rate
	 * 
	 * @param messageFailureRate
	 *            - a percentage represented as a double in the range [0,1]
	 */
	public void configureFailureDetection(double messageFailureRate) {
		if (messageFailureRate <= 0.0 || messageFailureRate > 1.0) {
			FailureDetectorClient.messageFailureRate = 0.0;
		} else {
			FailureDetectorClient.messageFailureRate = messageFailureRate;
		}
		this.failureDetectorServer = new FailureDetectorServer();
		this.failureDetectorClient = new FailureDetectorClient();
		this.grepServer = new GrepServer();
		this.groupServer = new GroupServer();
	}

	/**
	 * The main driver function. This function calls sever preparation function,
	 * prompts the user for input, and delegates tasks based on the user's
	 * requests.
	 */
	public void run() {
		startGrepServer(); // listen for incoming grep requests.

		try {
			hostaddress = InetAddress.getLocalHost().getHostAddress();

			System.out.println("RemoteGrepApplication - Server started on: "
					+ hostaddress + ":" + grepServer.getPort());
		} catch (UnknownHostException e1) {
			LOGGER.warning("RemoteGrepApplication - run() - failed to identify host");
		}
		// If this is the introducer node, start listening for incoming requests
		if (hostaddress.equals(LINUX_5)) {
			synchronized (groupMembershipList) {
	
				// Add Linux5 as the first member
				Node newNode = new Node(System.currentTimeMillis(),
						hostaddress, Integer.valueOf(UDP_PORT));
				groupMembershipList.add(newNode);

				startGroupServer();
				LOGGER.info("RemoteGrepApplication - run() - Listening for join requests.");
				System.out.println("Group Server started. Listening for join requests.");
			}
		}

		InputStreamReader inputStreamReader = new InputStreamReader(System.in);
		BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
		String input = "";

		while (true) {
			promptUserForInput();

			try {
				input = bufferedReader.readLine();

				// Add a node to grep
				if ("a".equals(input.trim())) {
					System.out
							.println("Enter IP and port (e.g. \"1.2.3.4:4444\"): ");
					String ipAndPort = bufferedReader.readLine();
					addTaskForNode(new Node(ipAndPort));
				}
				// Add node to group membership list
				else if ("j".equals(input.trim())) {
					if (!groupMembershipList.contains(new Node(System
							.currentTimeMillis(), hostaddress, 4445))) {
						joinGroup();
					} else {
						System.out.println("Node " + hostaddress
								+ " is already part of the group.");
					}
				}
				// Add Default nodes to grep
				else if ("d".equals(input.trim())) {
					addDefaultNodes();
				} else if ("g".equals(input.trim())) {
					System.out.println(groupMembershipList);
				}
				// Run grep
				else if ("q".equals(input.trim())) {
					System.out.print("Enter grep regex>");
					String regex = bufferedReader.readLine();

					long start = System.currentTimeMillis();
					runGrepTasks(regex);
					joinGrepTasks();
					long end = System.currentTimeMillis();

					System.out.println("Total Time: " + (end - start) + "ms");
				}
				// Leave group list
				else if ("l".equals(input.trim())) {
					System.out.println("Leaving group by choice.");
					leaveGroup();
				}
				// Exit
				else if ("e".equals(input.trim())) {

					System.out.println("Stopping membership server.");
					stopGroupServer();
					System.out.println("Stopping membership client.");

					if (groupClient != null) {
						groupClient.interrupt();
						groupClient.stopClient();
						try {
							groupClient.join();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					}

					System.out.println("Stopping heartbeats.");
					failureDetectorServer.stop();
					failureDetectorClient.stop();
					stopGrepServer();
					break;
				}
			} catch (IOException e) {
				LOGGER.warning("RemoteGrepApplication - run() - failed to readline from the input");
			}
		}
		try {
			bufferedReader.close();
		} catch (IOException e) {
			LOGGER.warning("RemoteGrepApplication - run() - failed to close bufferedreader");
		}
		try {
			inputStreamReader.close();
		} catch (IOException e) {
			LOGGER.warning("RemoteGrepApplication - run() - failed to close inputstreamreader");
		}
	}

	/**
	 * 	Present menu to the user
	 */
	private void promptUserForInput() {
		synchronized (groupMembershipList) {
			if (groupMembershipList.contains(new Node(System
					.currentTimeMillis(), hostaddress, 1111))) {
				System.out.println("(l) Leave group");
			} else {
				System.out.println("(j) Join group");
			}
		}
		System.out.println("(g) Current membership list");
		System.out.println("(a) Add node ((d) adds default nodes)");
		System.out.println("(q) Query logs");
		System.out.println("(e) Exit");
	}

	/**
	 * Creates a grep task for each of the default linux nodes (linux[5-7])
	 */
	public void addDefaultNodes() {
		for (String server : servers) {
			addTaskForNode(new Node(server));
		}
	}

	/**
	 * Start client that joins the membership group
	 */
	public void joinGroup() {
		groupClient = new GroupClient(new Node(LINUX_5 + ":" + UDP_PORT));
		groupClient.start();
	}

	/**
	 * Voluntarily leave the group. This method notifies other nodes that it is
	 * leaving so they can quickly remove them from their list.
	 */
	public void leaveGroup() {
		System.out.println("Stopping membership server.");
		stopGroupServer();
		System.out.println("Stopping membership client.");
		groupClient.stopClient();
		System.out.println("Stopping heartbeats.");
		failureDetectorServer.stop();
		failureDetectorClient.stop();

		try {
			Node thisNode = new Node(111L, hostaddress, 1111);

			System.out.println("Notify all nodes that I'm leaving.");
			if (groupMembershipList.contains(thisNode)) {
				// Tell all nodes to remove this node from their lists
				broadcast(groupMembershipList.get(groupMembershipList
						.indexOf(thisNode)), "R");
			}
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		groupMembershipList.clear();
		System.out.println("Group membership list cleared on this node.");
	}

	/**
	 * Waits for all of the grep tasks to complete and prints their results to
	 * the console
	 */
	public void joinGrepTasks() {
		for (GrepTask grepTask : grepTasks) {
			try {
				grepTask.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println("From node: " + grepTask.getNode().toString());
			System.out.print(grepTask.getResult());
		}
		grepTasks.clear();
		System.out
				.println("Grep Successful: Removed all nodes. Please re-add nodes you would like to search again.");
	}

	/**
	 * So we can run all of the setup grep tasks for each server.
	 * 
	 * @param regex
	 *            - grep command to search for. This may include flags at the
	 *            beginning. (Ex. -rni severe)
	 */
	public void runGrepTasks(String regex) {
		for (GrepTask grepTask : grepTasks) {
			grepTask.setRegex(regex);
			grepTask.start();
		}
	}

	/**
	 * We need to store which servers to run grep tasks on.
	 * 
	 * @param node
	 *            - Server to create a grep task for
	 */
	public void addTaskForNode(Node node) {
		grepTasks.add(new GrepTask(node));
	}

	/**
	 * Start group server to listen for incoming join requests
	 */
	public void startGroupServer() {
		groupServer.start();
	}

	/**
	 * Interrupt the group server
	 */
	public void stopGroupServer() {
		if (groupServer.isAlive()) {
			groupServer.stopServer();
			try {
				groupServer.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
				LOGGER.warning("Could not join GroupServer thread.");
			}
		}
	}

	/**
	 * Listens on port default port for incoming grep requests
	 */
	public void startGrepServer() {
		grepServer.start();
	}

	/**
	 * Sends quit message to the grep server instance and waits for the grep
	 * sever thread to complete.
	 */
	public void stopGrepServer() {
		taskToStopServer = new GrepTask(new Node("localhost", 4444));
		taskToStopServer.setRegex("<QUIT>");
		taskToStopServer.start();
		try {
			taskToStopServer.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
			LOGGER.warning("RemoteGrepApplication - stopGrepServer - interrupted while joining task thread.");
		}
		try {
			grepServer.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
			LOGGER.warning("RemoteGrepApplication - stopGrepServer - interrupted while joining processing threads.");
		}
	}

	public static void printUsage() {
		System.out
				.println("USAGE: java -cp bin org.uiuc.cs.distributed.grep.RemoteGrepApplication <LOG_LOCATION>");
		System.out.println("\n");
	}

	/**
	 * Sends a multicast message to all nodes telling them to perform an action
	 * on this node in their list.
	 * 
	 * @param node
	 *            Notify others about some change that happened to this node
	 * @param action
	 *            Could be an Add "A" or Remove "R" event
	 * @throws UnknownHostException
	 * @throws IOException
	 */
	public void broadcast(Node node, String action)
			throws UnknownHostException, IOException {
		// Notify all nodes of group list change
		byte[] nodeChangedBuffer = new byte[256];
		DatagramSocket socket = new DatagramSocket(
				RemoteGrepApplication.UDP_MC_PORT);
		String nodeChangedMessage = action + ":" + node;
		nodeChangedBuffer = nodeChangedMessage.getBytes();

		System.out.println("Notifying group about membership change: "
				+ nodeChangedMessage);
		InetAddress group = InetAddress
				.getByName(RemoteGrepApplication.UDP_MC_GROUP);
		DatagramPacket groupListPacket = new DatagramPacket(nodeChangedBuffer,
				nodeChangedBuffer.length, group,
				RemoteGrepApplication.UDP_MC_PORT);
		socket.send(groupListPacket);
		socket.close();
	}

	/**
	 * Entry function for running the application
	 * 
	 * @param args
	 *            - used to read in the new logs location
	 */
	public static void main(String[] args) {
		if (args.length == 1) {
			// TODO: check to see if the location exists
			RemoteGrepApplication app = new RemoteGrepApplication(args[0]);
			app.configureFailureDetection(0);
			app.run();
		} else if (args.length == 2) {
			RemoteGrepApplication app = new RemoteGrepApplication(args[0]);
			app.configureFailureDetection(Double.parseDouble(args[1]));
			app.run();
		} else {
			printUsage();
			System.exit(-1);
		}

	}
}
