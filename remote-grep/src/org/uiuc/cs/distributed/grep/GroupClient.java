package org.uiuc.cs.distributed.grep;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;

/**
 * This class is responsible for sending the join request to an introducer (in
 * our case, linux5). It then listens to recieve broadcast about group
 * membership changes.
 * 
 * @author matt
 * 
 */
public class GroupClient extends Thread {
	private Node node;
	private MulticastSocket socket = null;
	InetAddress group = null;
	
	GroupClient(Node node) {
		super("GroupClient");
		this.node = node;
	}

	/**
	 * Interrupt client and clean up sockets.
	 */
	public void stopClient()
	{
		super.interrupt();
		this.interrupt();
		try {
			if(socket != null)
			{
				socket.leaveGroup(group);
				System.out.println("Closing client multicast socket.");
				socket.close();
			}
		} catch (IOException e) {
			System.out.println("Already have left group.");
		}
	}
	
	/**
	 * Asks to join LINUX_5 group and join multicast UDP group
	 */
	public void run() {
		boolean result = sendJoinRequest();
		if (result && !Application.getInstance().group.list.isEmpty()) {
			try {
				socket = new MulticastSocket(Application.UDP_MC_PORT);
				group = InetAddress.getByName(Application.UDP_MC_GROUP);
				
				socket.joinGroup(group);
				System.out.println("Joining UDP Group "+Application.UDP_MC_GROUP);
				Application.LOGGER.info("GroupClient - run() - started multicast UDP server on port: " +
								Application.UDP_MC_PORT);
				Application.LOGGER.info("RQ1: GroupClient - run() - joined multicast group: "+
								Application.UDP_MC_GROUP);
				
				while(!Thread.currentThread().isInterrupted()){
					byte[] buf = new byte[256];
					DatagramPacket packet = new DatagramPacket(buf, buf.length);
					System.out.println("Waiting to recieve broadcast.");
					Application.LOGGER.info("GroupClient - run() - Waiting to receive broadcast");

					try {
						socket.receive(packet);
					} catch (IOException e) {
						Application.LOGGER.info("GroupClient - run() - Receiving membership updates interrupted");
						System.out.println("Receiving membership updates interrupted.");
						break;
					} 
					String message = new String(packet.getData());

					System.out.println("It's a new membership update!");

					Node updatedNode = parseNodeFromMessage(message);
					String action = parseActionFromMessage(message);

					if(action != null && updatedNode != null) 
					{
						synchronized(Application.getInstance().group)
						{
							if (action.equals("A")) {
								Application.LOGGER.info("RQ1: GroupClient - run() - Adding node " + updatedNode + " to list.");
								Application.getInstance().group.add(updatedNode);
							} else if (action.equals("R")) {
								Application.LOGGER.info("RQ1: GroupClient - run() - Removing node " + updatedNode + " from list.");
								Application.getInstance().group.remove(updatedNode);
							}
							
							Application.LOGGER.info("GroupClient - run() - Updated membership list: " + Application.getInstance().group.toString());
							System.out.println("Updated membership list: " + Application.getInstance().group.list);
						}
					}
				}
			} catch (IOException e1) {
				System.out.println("Leaving group.");
				Application.LOGGER.info("GroupClient - run() - Leaving group.");
			} finally {
				try {
					socket.leaveGroup(group);
				} catch (IOException e) {
					System.out.println("Already have left the UDP group.");
					Application.LOGGER.info("GroupClient - run() - Already have left the UDP group.");
				}
				socket.close();
			}	
		}
	}

	/**
	 * Sends a message to Linux7 asking to be added to the group memebership
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
			address = InetAddress.getByName(node.getIP());
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

		// Receive current groupmembership list from Linux5
		receiveGroupList();
		return true;
	}

	/**
	 * Receives a group list from linux5. An END message will signify the completion of message
	 */
	private void receiveGroupList() {
		boolean receivingGroupList = true;
		byte[] receiveBuffer = new byte[256];

		DatagramSocket socket = null;

		try {
			// TODO: revise - this is likely an issue since GroupServer runs on the same port
			socket = new DatagramSocket(Application.UDP_PORT); 
			Application.LOGGER.info("GroupClient - receiveGroupList() - started server on: "+Application.UDP_PORT);
		} catch (SocketException e1) {
			e1.printStackTrace();
			Application.LOGGER.severe("GroupClient - receiveGroupList() - failed to start server on: "+Application.UDP_PORT);
			return;
		}

		while (receivingGroupList || !Thread.currentThread().isInterrupted()) {
			DatagramPacket addGroupMemberPacket = new DatagramPacket(
					receiveBuffer, receiveBuffer.length);
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
					break;
				}
			}


			Node nodeToAdd = parseNodeFromMessage(addGroupMemberMessage);
			synchronized(Application.getInstance().group)
			{
				Application.getInstance().group.add(nodeToAdd);
			}
		}

		socket.close();
	}

	/**
	 * Takes message and creates a {@link Node} out of it.
	 * 
	 * @param addGroupMemberMessage
	 *            ACTION:TIMESTAMP:IP:PORT
	 * @return Node from message
	 */
	private Node parseNodeFromMessage(String addGroupMemberMessage) {
		Application.LOGGER.info("Parsing message: " + addGroupMemberMessage);
		String[] parts = addGroupMemberMessage.split(":");
		if(parts.length != 4)
		{
			return null;
		}
		return new Node(Long.parseLong(parts[1]), parts[2], Integer.valueOf(parts[3].trim()));
	}
	
	/**
	 * Gets the action that a message wants to be performed. This is the first
	 * value of the message. 
	 * 
	 * @param addGroupMemberMessage
	 *            ACTION:TIMESTAMP:IP:PORT
	 * @return Action to perform. Null if message is not in correct format.
	 */
	private String parseActionFromMessage(String addGroupMemberMessage) {
		String[] parts = addGroupMemberMessage.split(":");
		if(parts.length != 4)
		{
			return null;
		}
		
		return parts[0];
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
			DatagramSocket socket = new DatagramSocket(Application.UDP_PORT);
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