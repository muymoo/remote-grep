package org.uiuc.cs.distributed.grep;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

/**
 * This is the join server that will be run on the introducer node. All join requests are handled by this node.
 * @author matt
 */
public class GroupServer extends Thread {
	protected DatagramSocket socket = null;
	protected volatile boolean alive = true;
	private boolean attemptingJoin = false;
	public boolean joinedGroup = false;

	public GroupServer() {
		this("GroupServerThread");
	}

	public GroupServer(String name)  {
		super(name);

	}

	/**
	 * Interrupt server and clean up sockets.
	 */
	public void stopServer()
	{
		alive = false;
		super.interrupt();
		socket.close();
	}

	/**
	 * Start listening for join requests. Either add the new node or swap it for
	 * an older instance in the case where the timestamps are different.
	 */
	public void run() {
		try {
			socket = new DatagramSocket(Application.UDP_PORT);
		} catch (SocketException e1) {
			Application.LOGGER.severe("GroupServer - Socket is already in use: " + Application.UDP_PORT);
			return;
		}

		boolean receivingGroupList = true;
		while (alive) {
			try {
				byte[] buf = new byte[256];
				
				// If this is the introducer node, start listening for incoming requests
				if (Application.hostaddress.equals(Application.INTRODUCER_IP)) {
					joinedGroup = true;

					// receive join request
					System.out.println("Waiting to recieve join requests. (e) to exit");
					
					DatagramPacket packet = new DatagramPacket(buf, buf.length);
					socket.receive(packet);
					String timestamp = new String (packet.getData(), 0, packet.getLength(), "UTF-8");
					System.out.println("timestamp: "+timestamp);
					// Add node to group list				
					Application.LOGGER.info("RQ1: GroupServer - run() - Join request recieved");
					Node newNode=null;
					try {
						newNode = new Node(Long.parseLong(timestamp), packet.getAddress().getHostAddress(), packet.getPort());
					} catch(NumberFormatException e) {
						Application.LOGGER.severe("GroupServer - run() - parseLong failed for string: "+timestamp);
					}
					synchronized(Application.getInstance().group.list)
					{
						if(!Application.getInstance().group.list.contains(newNode))
						{
							System.out.println("We have not seen this node before, let's add it.");
							addNewNode(newNode);
						}
						else
						{
							System.out.println("This node is trying to rejoin, it must have crashed before.");
							Node oldNode = Application.getInstance().group.list.get(Application.getInstance().group.list.indexOf(newNode));
							// If the newNode's time stamp is greater than the old one, add it to the list
							if(newNode.timestampCompareTo(oldNode) > 0)
							{
								// Replace old node with new node otherwise, ignore the new node, it must have been stuck in network land
								removeNode(oldNode);
								addNewNode(newNode);
							} 
							else 
							{
								System.out.println("Ignored node join request (too old): " + newNode.toString());
							}	
						}
					}
				
				} else {
					if(receivingGroupList && attemptingJoin)
					{
						DatagramPacket addGroupMemberPacket = new DatagramPacket(
								buf, buf.length);
						
						try {
							socket.setSoTimeout(5000);
						} catch (SocketException e1) {
							e1.printStackTrace();
						}
						try {
							socket.receive(addGroupMemberPacket);
						} catch (SocketTimeoutException timedOut)
						{
							System.out.println("linux5 is not available to accept join requests. Node will not be added.");
							break;
						} catch (IOException e) {
							Application.LOGGER.severe("GroupClient - rec");
						}
						try {
							socket.setSoTimeout(0);
						} catch (SocketException e) {
							e.printStackTrace();
						}
						
						String addGroupMemberMessage = new String(
								addGroupMemberPacket.getData(), 0,
								addGroupMemberPacket.getLength());
	
						synchronized(Application.getInstance().group)
						{
							if (addGroupMemberMessage.equalsIgnoreCase("END")) {
								System.out.println("Recieved updated group list: "
										+ Application.getInstance().group.toString());
								receivingGroupList = false;
								joinedGroup = true;
								attemptingJoin = false;
							} else {
								Node nodeToAdd = Application.getInstance().groupClient.parseNodeFromMessage(addGroupMemberMessage);
								Application.getInstance().group.add(nodeToAdd);
							}
						}
					}
				}

			} catch (IOException e) {
				alive = false;
			}
		}
		socket.close();
	}
	
	public void joinGroup()
	{
		attemptingJoin = true;
		boolean result = sendJoinRequest();
		
	}
	

	/**
	 * Sends a message to the introducer node asking to be added to the group memebership
	 * list
	 */
	private boolean sendJoinRequest() {
		System.out.println("Sending join request");
		// So we can uniquely identify this node (we can get IP/port from packet
		// by default)
		String joinRequest = String.valueOf(System.currentTimeMillis());
		Application.LOGGER.info("RQ1: GroupClient - sendJoinRequest() - Sending join request: " + joinRequest);
		
		InetAddress address = null;
		try {
			address = InetAddress.getByName(Application.INTRODUCER_IP);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

		// send the join request
		try {
			sendData(address, joinRequest);
		} catch (IOException e) {
			e.printStackTrace();
			Application.LOGGER.warning("RQ1: GroupClient - sendJoinRequest() - Join request failed. Is linux5 down?");
			return false;
		}

		return true;
	}

	/**
	 * Removes a node from local list and sends updated to other nodes.
	 * 
	 * @param oldNode Node to remove from membershiplist
	 */
	private void removeNode(Node oldNode) {
		synchronized(Application.getInstance().group)
		{
			Application.getInstance().group.remove(oldNode);
		}
		Application.LOGGER.info("RQ1: GroupServer - removeNode() - Old node removed successfully: " + oldNode);
		System.out.println("Everyone should remove: " + oldNode);
		
		try {
			broadcast(oldNode, "R");
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Sends a multicast message to all nodes telling them to perform an action on this node in their list.
	 * 
	 * @param node Notify others about some change that happened to this node
	 * @param action Could be an Add "A" or Remove "R" event
	 * @throws UnknownHostException
	 * @throws IOException
	 */
	public void broadcast(Node node, String action) throws UnknownHostException,
			IOException {
		// Notify all nodes of group list change
		byte[] nodeChangedBuffer = new byte[256];
		String nodeChangedMessage = action + ":" + node;
		nodeChangedBuffer = nodeChangedMessage.getBytes();

		if(!FailureDetectorClient.isRandomFailure())
		{
			Application.LOGGER.info("RQ1: GroupServer - broadcast() - Broadcasting updated to all nodes in UDP group.");
			System.out.println("Notifying group about membership change: " + nodeChangedMessage);
			InetAddress group = InetAddress.getByName(Application.UDP_MC_GROUP);
			DatagramPacket groupListPacket = new DatagramPacket(
					nodeChangedBuffer, nodeChangedBuffer.length, group, Application.UDP_MC_PORT);
			socket.send(groupListPacket);
		}
	}

	/**
	 * Sends a full group list to the new node.
	 * 
	 * @param newNode Node that needs a complete list
	 * @throws IOException
	 */
	private void sendGroupList(Node newNode) throws IOException {
		byte[] buf;
		InetAddress address = InetAddress.getByName(newNode.getIP());
		String addNodeMessage = "";
		System.out.println("Sending current group list to new node: " + address.getHostAddress());
		
		// The new node doesn't have any members! Send current group list to new member. 
		synchronized(Application.getInstance().group.list)
		{
			for(Node node : Application.getInstance().group.list)
			{
				if(!FailureDetectorClient.isRandomFailure())
				{
					addNodeMessage = "A:"+node.toString();
					buf = addNodeMessage.getBytes();
					DatagramPacket addNodePacket = new DatagramPacket(buf, buf.length, address,
							Application.UDP_PORT);
					System.out.println("Sending: " + addNodeMessage);
					socket.send(addNodePacket);
				}
			}
		}

		if(!FailureDetectorClient.isRandomFailure())
		{
			System.out.println("Sending END packet");
			String endMessage = "END";
			buf = endMessage.getBytes();
			DatagramPacket endPacket = new DatagramPacket(buf, buf.length, address,
					Application.UDP_PORT);
			socket.send(endPacket);
		}
	}

	/**
	 * Updates local membership list and broadcasts add node message to all other members
	 * 
	 * @param newNode Node to add to group
	 */
	private void addNewNode(Node newNode) {
		String groupList;
		synchronized(Application.getInstance().group)
		{
			Application.getInstance().group.add(newNode);
			groupList = Application.getInstance().group.toString();
		}
		System.out.println("New Node added successfully: " + newNode.toString());
		System.out.println("Updated Group list: " + groupList);
		Application.LOGGER.info("RQ1: GroupServer - addNewNode() - New node added successfully: " + newNode);
		
		try {
			sendGroupList(newNode);
			broadcast(newNode, "A");
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	

	/** 
	 * Send a string of data to an address over UDP
	 * 
	 * @param target Target address
	 * @param data Content to send to target
	 */
	private void sendData(InetAddress target, String data) throws IOException {
		if(!FailureDetectorClient.isRandomFailure())
		{
			DatagramSocket socket = new DatagramSocket();
			System.out.println("sendData: " + data);
			if (data.getBytes("UTF-8").length > 256) {
				Application.LOGGER
						.warning("GroupClient - sendData() - data string: " + data
								+ " is too long.");
				socket.close();
				throw new IOException();
			}
			byte[] buf = new byte[256];
			buf = data.getBytes("UTF-8");
			
			DatagramPacket datagram = new DatagramPacket(data.getBytes("utf-8"),
					buf.length, target, Application.UDP_PORT);
			socket.send(datagram);
			socket.close();
		}
	}
}
