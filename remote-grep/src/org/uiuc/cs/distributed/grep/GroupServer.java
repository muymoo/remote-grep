package org.uiuc.cs.distributed.grep;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.List;

public class GroupServer extends Thread {
	protected DatagramSocket socket = null;
	protected volatile boolean alive = true;

	public GroupServer() {
		this("GroupServerThread");
	}

	public GroupServer(String name)  {
		super(name);
	}

	public void stopServer()
	{
		alive = false;
		super.interrupt();
		socket.close();
	}
	
	public void run() {
		try {
			socket = new DatagramSocket(RemoteGrepApplication.UDP_PORT);
		} catch (SocketException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		while (alive) {
			try {
				byte[] buf = new byte[256];

				// receive join request
				System.out.println("Waiting to recieve join requests. Hit me up. (e) to exit");
				DatagramPacket packet = new DatagramPacket(buf, buf.length);
				socket.receive(packet);
				System.out.println("Oooh, somebody wants to join the party.");
				String timestamp = new String (packet.getData(), 0, packet.getLength(), "UTF-8");
				
				// Add node to group list				
				System.out.println("Timestamp added: " + timestamp);
				Node newNode = new Node(Long.parseLong(timestamp), packet.getAddress().getHostAddress(), packet.getPort());
				
				synchronized(RemoteGrepApplication.groupMembershipList)
				{
					if(!RemoteGrepApplication.groupMembershipList.contains(newNode))
					{
						System.out.println("We have not seen this node before, let's add it.");
						addNewNode(newNode);
					}
					else
					{
						System.out.println("This node is trying to rejoin, it must have crashed before.");
						Node oldNode = RemoteGrepApplication.groupMembershipList.get(RemoteGrepApplication.groupMembershipList.indexOf(newNode));
						// If the newNode's time stamp is greater than the old one, add it to the list
						if(newNode.compareTo(oldNode) > 0)
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
				

				
				System.out.println("Let's see if anyone else shows up.");

			} catch (IOException e) {
				alive = false;
			}
		}
		socket.close();
	}

	/**
	 * Removes a node from local list and sends updated to other nodes.
	 * 
	 * @param oldNode Node to remove from membershiplist
	 */
	private void removeNode(Node oldNode) {
		synchronized(RemoteGrepApplication.groupMembershipList)
		{
			RemoteGrepApplication.groupMembershipList.remove(oldNode);
		}
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

		System.out.println("Notifying group about membership change: " + nodeChangedMessage);
		InetAddress group = InetAddress.getByName(RemoteGrepApplication.UDP_MC_GROUP);
		DatagramPacket groupListPacket = new DatagramPacket(
				nodeChangedBuffer, nodeChangedBuffer.length, group, RemoteGrepApplication.UDP_MC_PORT);
		socket.send(groupListPacket);
	}

	private void sendGroupList(Node newNode) throws IOException {
		byte[] buf;
		InetAddress address = InetAddress.getByName(newNode.getIP());
		String addNodeMessage = "";
		System.out.println("Sending current group list to new node: " + address.getHostAddress());
		
		// The new node doesn't have any members! Send current group list to new member. 
		synchronized(RemoteGrepApplication.groupMembershipList)
		{
			for(Node node : RemoteGrepApplication.groupMembershipList)
			{
				addNodeMessage = "A:"+node.toString();
				buf = addNodeMessage.getBytes();
				DatagramPacket addNodePacket = new DatagramPacket(buf, buf.length, address,
						RemoteGrepApplication.UDP_PORT);
				System.out.println("Sending: " + addNodeMessage);
				socket.send(addNodePacket);
			}
		}

		
		System.out.println("Sending END packet");
		String endMessage = "END";
		buf = endMessage.getBytes();
		DatagramPacket endPacket = new DatagramPacket(buf, buf.length, address,
				RemoteGrepApplication.UDP_PORT);
		socket.send(endPacket);
	}

	private void addNewNode(Node newNode) {
		String groupList;
		synchronized(RemoteGrepApplication.groupMembershipList)
		{
			RemoteGrepApplication.groupMembershipList.add(newNode);
			groupList = RemoteGrepApplication.groupMembershipList.toString();
		}
		System.out.println("New Node added successfully: " + newNode.toString());
		System.out.println("Group list: " + groupList);
		
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

}
