package org.uiuc.cs.distributed.grep;


import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class FailureDetectorServer {
	protected static BlockingQueue<Node> heartbeatQueue;
	private Thread producer;
	private Thread consumer;
	private String hostaddress = "";
	
	public FailureDetectorServer()
	{
		try {
			hostaddress = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}
		
		heartbeatQueue = new ArrayBlockingQueue<Node>(6);
		producer = new Thread(new HeartbeatProducer());
		consumer = new Thread(new HeartbeatConsumer());
		producer.start();
		consumer.start();
	}
	
	public void stop()
	{
		producer.stop();
		consumer.stop();
		// TODO: alternative is to send a poison datagram to producer,
		//       which adds poison item to queue.
	}

	
	public class HeartbeatConsumer implements Runnable
	{

		@Override
		public void run() {
			RemoteGrepApplication.LOGGER.info("FailureDetectorServer - consumer.run() - starting consumer");
			while(true)
			{
				// sleep 2.5 seconds
				long start = new Date().getTime();
				long end = start;
				while((end - start) < 2500)
				{
					try {
						Thread.sleep(2500);
					} catch (InterruptedException e) {
					}
					end = new Date().getTime();
				}
				
				// process items in heartbeat queue
				synchronized(heartbeatQueue)
				{
					while(heartbeatQueue.size() > 0)
					{
						
					}
				}
				
				// iterate over other group members to detect failures
				long currTime = new Date().getTime();
				synchronized(RemoteGrepApplication.groupMembershipList)
				{				
					Iterator<Node> i = RemoteGrepApplication.groupMembershipList.iterator(); // Must be in synchronized block
				    while (i.hasNext())
				    {
				    	Node node = i.next();
						if(!node.isSelf(hostaddress))
						{
							long nodeLastUpdate = Long.parseLong(node.getTimestamp());
							
							if((nodeLastUpdate+RemoteGrepApplication.timeBoundedFailureInMilli) >
									currTime)
							{
								RemoteGrepApplication.LOGGER.info(new Date().getTime()+" RQ1: FailureDetectorServer - run() - failure detected at node: "+ node.toString());
								RemoteGrepApplication.LOGGER.info(" RQ1: FailureDetectorServer - run() - removing failed node.");
								System.out.println(new Date().getTime()+" failure detected at node: "+node.toString());
								// remove from list
								System.out.println("Removing node: "+node.toString());
								RemoteGrepApplication.groupMembershipList.remove(node);
							}
						}
				    }
				}
			}
		}
		
		

		
	}
	
	public class HeartbeatProducer implements Runnable
	{
		protected DatagramSocket socket = null;

		@Override
		public void run() {
			RemoteGrepApplication.LOGGER.info("FailureDetectorServer - producer.run() - starting producer");
			try {
				socket = new DatagramSocket(RemoteGrepApplication.UDP_FD_PORT);
			} catch (SocketException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				RemoteGrepApplication.LOGGER.info("FailureDetectorServer - producer.run() - couldn't start server on port: "+
						RemoteGrepApplication.UDP_FD_PORT);
			}
			
			while(true)
			{
				
				byte[] buf = new byte[256];
				DatagramPacket packet = new DatagramPacket(buf, buf.length);
				try {
					socket.receive(packet);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
		    	try {
					String data = new String(packet.getData(),0, packet.getLength(), "UTF-8");
					if(data.equals("HEARTBEAT"))
					{
						
					}
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
	}
	
}
