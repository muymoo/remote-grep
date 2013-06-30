package org.uiuc.cs.distributed.grep;


import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
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
	protected DatagramSocket socket = null;
	
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
		socket.close();
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
				
				// creating a local copy of the queue elements to avoid a deadlock situation
				// with nested synchronized statements
				List<Node> heartbeatsToProcess = new ArrayList<Node>();
				synchronized(heartbeatQueue)
				{
					RemoteGrepApplication.LOGGER.info("FailureDetectorServer - consumer.run() - heartbeatQueue size: "+heartbeatQueue.size());
					while(heartbeatQueue.size() > 0)
					{
						Node temp = heartbeatQueue.remove();
						heartbeatsToProcess.add(temp);
						RemoteGrepApplication.LOGGER.info("FailureDetectorServer - consumer.run() - heartbeatQueue element: "+temp.toString());
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
							// process heartbeat queue updates
							RemoteGrepApplication.LOGGER.info("FailureDetectorServer - consumer.run() - starting to process heartbeat queue updates");
							int equalsComparisons = 0;
							for(Node updateNode : heartbeatsToProcess)
							{
								if(updateNode.equals(node))
								{
									equalsComparisons++;
									// the heartbeat timestamp is larger than the current one
									if(updateNode.compareTo(node) > 0)
									{
										node.setTimestamp(updateNode.getTimestamp());
									}
								}
							}
							if(equalsComparisons != heartbeatsToProcess.size())
							{
								RemoteGrepApplication.LOGGER.warning("FailureDetectorServer - consumer.run() - some of the heartbeats in the queue didn't match the membership list");
							}
							
							long nodeLastUpdate = Long.parseLong(node.getTimestamp());
							
							if((nodeLastUpdate+RemoteGrepApplication.timeBoundedFailureInMilli) <
									currTime)
							{
								RemoteGrepApplication.LOGGER.info(new Date().getTime()+" RQ1: FailureDetectorServer - run() - failure detected at node: "+ node.toString());

								System.out.println(new Date().getTime()+" failure detected at node: "+node.toString());
								// remove from list
								RemoteGrepApplication.LOGGER.info(" RQ1: FailureDetectorServer - run() - removing failed node.");
								System.out.println("Removing node: "+node.toString());

								// Remove node from list (must use iterator since that's how we're looping)
								i.remove();
								
								try {
									// Notify others that node has been removed.
									broadcast(node, "R");
								} catch (UnknownHostException e) {
									e.printStackTrace();
								} catch (IOException e) {
									e.printStackTrace();
								}
							}
						}
				    }
				}
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
		private void broadcast(Node node, String action) throws UnknownHostException,
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

		
	}
	
	public class HeartbeatProducer implements Runnable
	{

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
					RemoteGrepApplication.LOGGER.info("FailureDetectionServer - producer.run() - received packet data: "+data);
					
					
					if(data.equals("HEARTBEAT"))
					{
						synchronized(heartbeatQueue)
						{
							Node heartbeatNode = new Node(String.valueOf(new Date().getTime()),packet.getAddress().toString().replace("/",""),packet.getPort());
							heartbeatQueue.add(heartbeatNode);
							RemoteGrepApplication.LOGGER.info("FailureDetectorServer - producer.run() - added heartbeat node: "+heartbeatNode.toString());
						}
					}
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
	}
	
}
