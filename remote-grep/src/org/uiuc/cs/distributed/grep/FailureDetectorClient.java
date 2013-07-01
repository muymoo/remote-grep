package org.uiuc.cs.distributed.grep;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class FailureDetectorClient{

	public DatagramSocket socket = null;
	private String hostaddress = "";
	private Thread client;
	public FailureDetectorClient() {
		try {
			hostaddress = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}
		client = new Thread(new FailureDetectorClientRunnable(),"FailureDetectorClient");
		client.start();
	}
	
	public void stop()
	{
		client.interrupt();
		try {
			client.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		socket.close();
		// TODO; 
	}
	
	public class FailureDetectorClientRunnable implements Runnable 
	{
	
		public void run()
		{
			// get a datagram socket
			try {
				socket = new DatagramSocket();
			} catch (SocketException e) {
				e.printStackTrace();
			}
			
			while(!Thread.currentThread().isInterrupted()) {
				
				// sleep
				long start = new Date().getTime();
				long end = start;
				while((end - start) < 2500 && !Thread.currentThread().isInterrupted())
				{
					try {
						Thread.sleep(2500);
					} catch (InterruptedException e) {
						return;
					}
					end = new Date().getTime();
				}
				
				synchronized(RemoteGrepApplication.groupMembershipList)
				{		
					// send heartbeats to all other nodes
					int nodesContacted = 0;

					Iterator<Node> i = RemoteGrepApplication.groupMembershipList.iterator(); // Must be in synchronized block
				    while (i.hasNext())
				    {
				    	Node node = i.next();
						if(!node.isSelf(hostaddress))
						{
							nodesContacted++;
							RemoteGrepApplication.LOGGER.info(new Date().getTime() +" FailureDetectorClient - run() - Sending heartbeat.");
							System.out.println("sending heartbeat");
							
							
							InetAddress target = null;
							try {
								target = InetAddress.getByName(node.getIP());
							} catch (UnknownHostException e) {
								e.printStackTrace();
							}
							
							try {
								sendData(target,"HEARTBEAT");
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
					}
					if(nodesContacted == 0) {
						RemoteGrepApplication.LOGGER.info(new Date().getTime()+" FailureDetectorClient - run() - no nodes to send heartbeat to");
					}
				}

			}
		}
		
	    private void sendData(InetAddress target, String data) throws IOException{
	    	if(data.getBytes("utf-8").length > 256) {
	    		RemoteGrepApplication.LOGGER.info("FailureDetectorClient - sendData - string data is too long");
	    		throw new IOException("string data is too long");
	    	}
	        DatagramPacket datagram = new DatagramPacket(data.getBytes("utf-8"), data.length(), target, RemoteGrepApplication.UDP_FD_PORT);
	        socket.send(datagram);
	    }
	}
}
