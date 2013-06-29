package org.uiuc.cs.distributed.grep;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;

public class GroupServer extends Thread {
	protected DatagramSocket socket = null;
	protected boolean alive = true;
	
	public GroupServer() {
		this("GroupServerThread");
	}

	public GroupServer(String name)  {
		super(name);
        
		try {
			socket = new DatagramSocket(4445);
		} catch (SocketException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}

	public void run() {

		while (alive) {
			try {
				byte[] buf = new byte[256];

				// receive request
				DatagramPacket packet = new DatagramPacket(buf, buf.length);
				socket.receive(packet);
				String timestamp = new String (packet.getData(), 0, packet.getLength());

				if(timestamp.equalsIgnoreCase("QUIT"))
				{
					System.out.println("GroupServer " + socket.getLocalAddress().toString() + " shutting down.");
					alive = false;
					break;
				}		
				
				// Add node to group list				
				System.out.println("Timestamp added: " + timestamp);
				RemoteGrepApplication.groupMemebershipList.add(timestamp + ":" +packet.getAddress().toString() + ":" + packet.getPort());
				System.out.println("Group list: " + RemoteGrepApplication.groupMemebershipList.toString());
				
				// Notify all nodes of group list change
				byte[] groupListBuffer = new byte[256];
				String groupList = RemoteGrepApplication.groupMemebershipList.toString();
				groupListBuffer = groupList.getBytes();
				
				for(String node : RemoteGrepApplication.groupMemebershipList){
					// Send updated membership list
					if(!node.equals(socket.getInetAddress()))
					{
						// Don't send updated list to itself
						break;
					}
					
					DatagramPacket groupListPacket = new DatagramPacket(groupListBuffer, groupListBuffer.length, InetAddress.getByName(node),
							4445);
					socket.send(groupListPacket);
				}
			} catch (IOException e) {
				e.printStackTrace();
				alive = false;
			}
		}
		socket.close();
	}

}
