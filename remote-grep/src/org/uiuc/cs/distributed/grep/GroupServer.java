package org.uiuc.cs.distributed.grep;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;

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
			socket = new DatagramSocket(4445);
		} catch (SocketException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		while (alive) {
			try {
				byte[] buf = new byte[256];

				// receive join request
				System.out.println("Waiting to recieve join requests. Hit me up.");
				DatagramPacket packet = new DatagramPacket(buf, buf.length);
				socket.receive(packet);
				System.out.println("Oooh, somebody wants to join the party.");
				String timestamp = new String (packet.getData(), 0, packet.getLength(), "UTF-8");

				if(timestamp.equalsIgnoreCase("QUIT"))
				{
					System.out.println("GroupServer " + socket.getLocalAddress().toString() + " shutting down.");
					alive = false;
					break;
				}		
				
				// Add node to group list				
				System.out.println("Timestamp added: " + timestamp);
				Node newNode = new Node(timestamp, packet.getAddress().toString(), packet.getPort());
				RemoteGrepApplication.groupMemebershipList.add(newNode);
				System.out.println("Group list: " + RemoteGrepApplication.groupMemebershipList.toString());
				
				
				InetAddress address = packet.getAddress();
				String addNodeMessage = "";
				System.out.println("Sending current group list to new node: " + address.toString());
				
				// The new node doesn't have any members! Send current group list to new member. 
				for(Node node : RemoteGrepApplication.groupMemebershipList)
				{
					addNodeMessage = "A:"+node.toString();
					buf = addNodeMessage.getBytes();
					DatagramPacket addNodePacket = new DatagramPacket(buf, buf.length, address,
							4445);
					System.out.println("Sending: " + addNodeMessage);
					socket.send(addNodePacket);
				}
				System.out.println("Sending END packet");
				String endMessage = "END";
				buf = endMessage.getBytes();
				DatagramPacket endPacket = new DatagramPacket(buf, buf.length, address,
						4445);
				socket.send(endPacket);
				
				// Notify all nodes of group list change
				byte[] groupListBuffer = new byte[256];
				System.out.println("Better tell my friends about who's here. I'll send a group text.");
				
				String addNewestNodeMessage = "A:" + newNode;
				groupListBuffer = addNewestNodeMessage.getBytes();

				System.out.println("Notifying group about membership change.");
				InetAddress group = InetAddress.getByName("228.5.6.7");

				DatagramPacket groupListPacket = new DatagramPacket(
						groupListBuffer, groupListBuffer.length, group, 4446);
				socket.send(groupListPacket);
				System.out.println("Group text complete. Let's see if anyone else shows up.");

			} catch (IOException e) {
				alive = false;
			}
		}
		socket.close();
	}

}
