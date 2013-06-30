package org.uiuc.cs.distributed.grep;

import java.io.File;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class GroupClient extends Thread {
	private Node node;
	private static Logger LOGGER;
	private static Handler logFileHandler;

	GroupClient(Node node) {
		this.node = node;
		String logFileLocation = RemoteGrepApplication.logLocation
				+ File.separator + "logs" + File.separator + "groupclient.log";
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
	}

	// Asks to join LINUX_5 group and join multicast group
	public void run() {
		boolean result = sendJoinRequest();
		if (result) {
			try {
				MulticastSocket socket = new MulticastSocket(4446);
				InetAddress group = InetAddress.getByName("228.5.6.7");
				socket.joinGroup(group);
				System.out.println("Joining UDP Group 228.5.6.7");
				
				byte[] buf = new byte[256];
				DatagramPacket packet = new DatagramPacket(buf, buf.length);
				System.out.println("Waiting to recieve broadcast");
				socket.receive(packet);
				System.out.println("Recieved new packet. I wonder what it could hold?");
				String timestamp = new String(packet.getData());
				
				// Update local groupmembership list
				System.out.println("It's a new membership list! I better update mine.");
				Node newNode = new Node(timestamp, packet.getAddress().toString(), packet.getPort());
				RemoteGrepApplication.groupMemebershipList.add(newNode);
				System.out.println(RemoteGrepApplication.groupMemebershipList);
				
				// Cleanup
				System.out.println("Leaving group. Peace.");
				socket.leaveGroup(group);
				socket.close();
			} 
			catch (IOException e1) 
			{
				e1.printStackTrace();
			}
		}

	}

	/**
	 * Sends a message to Linux7 asking to be added to the group memebership
	 * list
	 */
	private boolean sendJoinRequest() {
		// get a datagram socket
		DatagramSocket socket = null;
		try {
			socket = new DatagramSocket(4445);
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}

		// send request
		byte[] buf = new byte[256];

		// So we can uniquely identify this node (we can get IP/port from packet
		// by default)
		String joinRequest = String.valueOf(System.currentTimeMillis());
		buf = joinRequest.getBytes();

		InetAddress address = null;
		try {
			address = InetAddress.getByName(node.getIP());
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

		DatagramPacket packet = new DatagramPacket(buf, buf.length, address,
				4445);
		try {
			// Request to join group memebership list
			socket.send(packet);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// Receive current groupmembership list from Linux5
		boolean receivingGroupList = true;
		byte[] receiveBuffer = new byte[256];
		while(receivingGroupList)
		{
			
			DatagramPacket addGroupMemberPacket = new DatagramPacket(receiveBuffer, receiveBuffer.length);
			
			try {
				socket.receive(addGroupMemberPacket);
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			String addGroupMemberMessage = new String (addGroupMemberPacket.getData(), 0, addGroupMemberPacket.getLength());

			if(addGroupMemberMessage.equalsIgnoreCase("END"))
			{
				System.out.println("Recieved updated group list: " + RemoteGrepApplication.groupMemebershipList);
				receivingGroupList = false;
				break;
			}
			
			Node nodeToAdd = parseNodeFromMessage(addGroupMemberMessage);
			RemoteGrepApplication.groupMemebershipList.add(nodeToAdd);
		}
		
		socket.close();
		return true; 
	}

	/**
	 * Takes message and creates a {@link Node} out of it.
	 * 
	 * @param addGroupMemberMessage ACTION:TIMESTAMP:IP:PORT
	 * @return Node from message
	 */
	private Node parseNodeFromMessage(String addGroupMemberMessage) {
		System.out.println("Parsing message: " + addGroupMemberMessage);
		String[] parts = addGroupMemberMessage.split(":");
		System.out.println("Message parts: " + parts[1] + " " + parts[2] + " " + parts[3]);
		return new Node(parts[1], parts[2], Integer.valueOf(parts[3]));
	}
}