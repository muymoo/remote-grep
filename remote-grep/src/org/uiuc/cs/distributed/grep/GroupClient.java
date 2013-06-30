package org.uiuc.cs.distributed.grep;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.SocketException;
import java.net.UnknownHostException;

public class GroupClient extends Thread {
	private Node node;

	GroupClient(Node node) {
		this.node = node;
	}

	// Asks to join LINUX_5 group and join multicast group
	public void run() {
		boolean result = sendJoinRequest();
		if (result) {
			MulticastSocket socket = null;
			InetAddress group = null;
			try {
				socket = new MulticastSocket(4446);
				group = InetAddress.getByName("228.5.6.7");
				
				socket.joinGroup(group);
				System.out.println("Joining UDP Group 228.5.6.7");
				
				while(isAlive()){
					byte[] buf = new byte[256];
					DatagramPacket packet = new DatagramPacket(buf, buf.length);
					System.out
							.println("Waiting to recieve broadcast. Ctrl-C to stop server.");
					socket.receive(packet);
					System.out
							.println("Recieved new packet. I wonder what it could hold?");
					String message = new String(packet.getData());

					System.out.println("It's a new membership update!");

					Node updatedNode = parseNodeFromMessage(message);
					String action = parseActionFromMessage(message);

					if (action.equals("A")) {
						RemoteGrepApplication.groupMembershipList
								.add(updatedNode);
					} else if (action.equals("R")) {
						RemoteGrepApplication.groupMembershipList
								.remove(updatedNode);
					}

					System.out.println("Updated membership list: "
							+ RemoteGrepApplication.groupMembershipList);
				}
			} catch (IOException e1) {
				System.out.println("Leaving group. Peace.");

				e1.printStackTrace();
			} finally {
				try {
					socket.leaveGroup(group);
				} catch (IOException e) {
					e.printStackTrace();
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
			socket = new DatagramSocket(4445);
		} catch (SocketException e1) {
			e1.printStackTrace();
			return;
		}

		while (receivingGroupList) {
			DatagramPacket addGroupMemberPacket = new DatagramPacket(
					receiveBuffer, receiveBuffer.length);

			try {
				socket.receive(addGroupMemberPacket);
			} catch (IOException e) {
				e.printStackTrace();
			}

			String addGroupMemberMessage = new String(
					addGroupMemberPacket.getData(), 0,
					addGroupMemberPacket.getLength());

			if (addGroupMemberMessage.equalsIgnoreCase("END")) {
				System.out.println("Recieved updated group list: "
						+ RemoteGrepApplication.groupMembershipList);
				receivingGroupList = false;
				break;
			}

			Node nodeToAdd = parseNodeFromMessage(addGroupMemberMessage);
			RemoteGrepApplication.groupMembershipList.add(nodeToAdd);
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
		System.out.println("Parsing message: " + addGroupMemberMessage);
		String[] parts = addGroupMemberMessage.split(":");
		System.out.println("Message parts: " + parts[1] + " " + parts[2] + " "
				+ parts[3]);
		return new Node(parts[1], parts[2], Integer.valueOf(parts[3].trim()));
	}
	
	private String parseActionFromMessage(String addGroupMemberMessage) {
		String[] parts = addGroupMemberMessage.split(":");
		return parts[0];
	}

	/** 
	 * function for sending
	 */
	private void sendData(InetAddress target, String data) throws IOException {
		DatagramSocket socket = new DatagramSocket(4445);
		System.out.println("sendData: " + data);
		if (data.getBytes("UTF-8").length > 256) {
			RemoteGrepApplication.LOGGER
					.warning("GroupClient - sendData() - data string: " + data
							+ " is too long.");
			socket.close();
			throw new IOException();
		}
		byte[] buf = new byte[256];
		buf = data.getBytes("UTF-8");
		
		DatagramPacket datagram = new DatagramPacket(data.getBytes("utf-8"),
				buf.length, target, RemoteGrepApplication.UDP_PORT);
		System.out.println("Sending: " + datagram.toString());
		socket.send(datagram);
		socket.close();
	}
}