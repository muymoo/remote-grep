package org.uiuc.cs.distributed.grep;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.UnknownHostException;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class GroupClient extends Thread {
	private Node node;

	GroupClient(Node node) {
		this.node = node;
	}

	// Asks to join LINUX_7 group and joins multicast group
	public void run() {
		boolean result = sendJoinRequest();
		if (result) {
			try {
				MulticastSocket socket = new MulticastSocket(4446);
				InetAddress group = InetAddress.getByName("228.5.6.7");
				socket.joinGroup(group);
				System.out.println("Joining UDP Group");
				
				byte[] buf = new byte[256];
				DatagramPacket packet = new DatagramPacket(buf, buf.length);
				System.out.println("Waiting to recieve broadcast");
				socket.receive(packet);
				System.out.println("Recieved new packet. I wonder what it could hold?");
				String timestamp = new String(packet.getData());
				
				// Update local groupmembership list
				System.out.println("It's a new membership list! I better update mine.");
				RemoteGrepApplication.groupMemebershipList.add(timestamp + ":" +packet.getAddress().toString() + ":" + packet.getPort());
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
			sendData(address,joinRequest);
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}

		return true; // TODO Wait until success message from linux7 or time out
	}
	

	/*
	 * function for sending 
	 */
   private void sendData(InetAddress target, String data) throws IOException{
		DatagramSocket socket =new DatagramSocket();
		
		if(data.getBytes("UTF-8").length > 256) {
			RemoteGrepApplication.LOGGER.warning("GroupClient - sendData() - data string: "+data+" is too long.");
			throw new IOException();
		}
		byte[] buf = new byte[256];
		buf = data.getBytes("UTF-8");
		
        DatagramPacket datagram = new DatagramPacket(data.getBytes("utf-8"), buf.length, InetAddress.getLocalHost(),RemoteGrepApplication.UDP_PORT);
        socket.send(datagram);
   }
}