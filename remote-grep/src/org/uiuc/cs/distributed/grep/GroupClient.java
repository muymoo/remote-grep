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
	private MulticastSocket socket = null;
	InetAddress group = null;
	
	GroupClient() {
		super("GroupClient");
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
				Application.LOGGER.info("GroupClient - run() - Waiting to receive broadcast");

				try {
					socket.receive(packet);
				} catch (IOException e) {
					Application.LOGGER.info("GroupClient - run() - Receiving membership updates interrupted");
					System.out.println("Receiving membership updates interrupted.");
					break;
				} 
				
				if(Application.getInstance().groupServer.joinedGroup &&
						!packet.getAddress().getHostAddress().equals(Application.hostaddress))
				{
					String message = new String(packet.getData());
	
					System.out.println("Received membership update");
					
	
					Node updatedNode = parseNodeFromMessage(message);
					String action = parseActionFromMessage(message);
	
					if(action != null && updatedNode != null) 
					{
						synchronized(Application.getInstance().group)
						{
							if (action.equals("A")) {
								System.out.println("Received Add node message");
								Application.LOGGER.info("RQ1: GroupClient - run() - Adding node " + updatedNode + " to list.");
								Application.getInstance().group.add(updatedNode);
							} else if (action.equals("R")) {
								System.out.println("Received remove node message");
								Application.LOGGER.info("RQ1: GroupClient - run() - Removing node " + updatedNode + " from list.");
								Application.getInstance().group.remove(updatedNode);
							}
							
							Application.LOGGER.info("GroupClient - run() - Updated membership list: " + Application.getInstance().group.toString());
							System.out.println("Updated membership list: " + Application.getInstance().group.list);
						}
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

	/**
	 * Takes message and creates a {@link Node} out of it.
	 * 
	 * @param addGroupMemberMessage
	 *            ACTION:TIMESTAMP:IP:PORT
	 * @return Node from message
	 */
	public Node parseNodeFromMessage(String addGroupMemberMessage) {
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

}