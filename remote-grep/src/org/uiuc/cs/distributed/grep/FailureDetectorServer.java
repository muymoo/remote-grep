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

/*
 * This class performs the failure detector server work
 * the server receives on a UDP port waiting for heartbeat messages from other nodes.
 * This server uses the consumer/producer pattern to process heartbeat messages.
 * The constructor starts both consumer and producer threads.
 */
public class FailureDetectorServer {
	protected static BlockingQueue<Node> heartbeatQueue;
	private Thread producer;
	private Thread consumer;
	protected DatagramSocket socket = null;
	
	public FailureDetectorServer()
	{	
		try {
			socket = new DatagramSocket(Application.UDP_FD_PORT);
		} catch (SocketException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			Application.LOGGER.info("FailureDetectorServer - producer.run() - couldn't start server on port: "+
					Application.UDP_FD_PORT);
		}
		
		heartbeatQueue = new ArrayBlockingQueue<Node>(30);
		producer = new Thread(new HeartbeatProducer());
		consumer = new Thread(new HeartbeatConsumer());
		producer.start();
		consumer.start();
	}
	
	/*
	 * stops both the consumer/producer threads so that the application can exit
	 */
	public void stop()
	{
//		producer.stop();
//		consumer.stop();
		producer.interrupt();
		consumer.interrupt();
		socket.close();
		try {
			consumer.join();
			producer.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// TODO: alternative is to send a poison datagram to producer,
		//       which adds poison item to queue.
		
	}

	/*
	 * this runnable class acts as the consumer of heartbeast messages. It updates the timestamps
	 * in the nodes this node is monitoring, and will determine failures based on whether or not 
	 * updates have been received.
	 */
	public class HeartbeatConsumer implements Runnable
	{

		
		public HeartbeatConsumer()
		{
		}

		
		private List<Node> processQueue()
		{
			
			// creating a local copy of the queue elements to avoid a deadlock situation
			// with nested synchronized statements
			List<Node> heartbeatsToProcess = new ArrayList<Node>();
			synchronized(heartbeatQueue)
			{
				while(heartbeatQueue.size() > 0)
				{
					Node temp = heartbeatQueue.remove();
					heartbeatsToProcess.add(temp);
				}
			}
			
			return heartbeatsToProcess;
		}
		
		private void detectFailures(List<Node> heartbeatsToProcess)
		{
			// iterate over other group members to detect failures
			long currTime = new Date().getTime();
			synchronized(Application.getInstance().group.list)
			{				
				Node node = Application.getInstance().group.getHeartbeatReceiveNode();
				
				if(node != null)
				{
					// process heartbeat queue updates
					int equalsComparisons = 0;
					for(Node updateNode : heartbeatsToProcess)
					{
						if(updateNode.equals(node))
						{
							equalsComparisons++;
							if(updateNode.lastUpdatedCompareTo(node) > 0)
							{
								node.lastUpdatedTimestamp = updateNode.lastUpdatedTimestamp;
							}
						}
					}
					if(equalsComparisons != heartbeatsToProcess.size())
					{
						Application.LOGGER.warning("FailureDetectorServer - consumer.run() - some of the heartbeats in the queue didn't match the membership list");
					}
					
					// detect failures
					if(node.lastUpdatedTimestamp > 0)
					{
						if((node.lastUpdatedTimestamp+Application.timeBoundedFailureInMilli) <
								currTime)
						{
							Application.LOGGER.info(new Date().getTime()+" RQ1: FailureDetectorServer - run() - failure detected at node: "+ node.verboseToString());

							System.out.println("Failure detected at node: "+node.verboseToString());
							
							try {
								// Notify others that node has been removed.
								broadcast(node, "R");
							} catch (UnknownHostException e) {
								e.printStackTrace();
							} catch (IOException e) {
								e.printStackTrace();
							}
							
							// remove from list
							Application.LOGGER.info(" RQ1: FailureDetectorServer - run() - removing failed node.");
							
							
							if(Application.getInstance().group.isLeader())
							{
								Application.getInstance().dfsServer.removeFailedNodeEntries(node);
							}
							// Remove node from list
							Application.getInstance().group.remove(node);
						}
					}
			    }
			}
		}
	
		@Override
		public void run() {
			Application.LOGGER.info("FailureDetectorServer - consumer.run() - starting consumer");
			while(!Thread.currentThread().isInterrupted())
			{
				// check if a leader election is in progress
				boolean electionInProgress = false;
				synchronized(Application.getInstance().group.list)
				{
					electionInProgress = Application.getInstance().group.electionInProgress;
				}
				if(!electionInProgress)
				{
					// no election is in progress
					List<Node> heartbeatsToProcess = processQueue();
					detectFailures(heartbeatsToProcess);
				} else {
					processQueue();
					Application.getInstance().group.resetLastUpdated();
				}
				
				// sleep 2.5 seconds
				long start = new Date().getTime();
				long end = start;
				while((end - start) < 2500)
				{
					try {
						Thread.sleep(2500);
					} catch (InterruptedException e) {
						return;
					}
					end = new Date().getTime();
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
			InetAddress group = InetAddress.getByName(Application.UDP_MC_GROUP);
			DatagramPacket groupListPacket = new DatagramPacket(
					nodeChangedBuffer, nodeChangedBuffer.length, group, Application.UDP_MC_PORT);
			socket.send(groupListPacket);
		}
	}
	
	/*
	 * this runnable acts as the producer of heartbeat updates. It waits on receiving 
	 * updates over a UDP port. Once a message is received, it will add the update to
	 * the heartbeat queue for the consumer to process.
	 */
	public class HeartbeatProducer implements Runnable
	{

		/*
		 * This is the main run method for the producer, it is meant to be started 
		 * only once during the execution of the application.
		 * 
		 * @see java.lang.Runnable#run()
		 */
		@Override
		public void run() {
			Application.LOGGER.info("FailureDetectorServer - producer.run() - starting producer");
			
			while(!Thread.currentThread().isInterrupted())
			{
				
				byte[] buf = new byte[256];
				DatagramPacket packet = new DatagramPacket(buf, buf.length);
				try {
					socket.receive(packet);
				} catch (IOException e) {
					System.out.println("Receiving heartbeats interrupted.");
				}
				
		    	try {
					String data = new String(packet.getData(),0, packet.getLength(), "UTF-8");
					Application.LOGGER.info("FailureDetectorServer - producer.run() - received packet data: "+data);
					
					
					if(data.equals("HEARTBEAT"))
					{
						synchronized(heartbeatQueue)
						{
							Node heartbeatNode = new Node(System.currentTimeMillis(),packet.getAddress().toString().replace("/",""),packet.getPort(),System.currentTimeMillis());
							heartbeatQueue.add(heartbeatNode);
							Application.LOGGER.info("FailureDetectorServer - producer.run() - received heartbeat from node: "+heartbeatNode.getIP());
						}
					}
					else if(data.equals("ELECTION"))
					{
						Application.LOGGER.info("FailureDetectorServer - producer.run() - received ELECTION message");
						
						// set global election status
						synchronized(Application.getInstance().group)
						{
							Application.getInstance().group.electionInProgress = true;
						}
						
						// check to see how the sending node ranks to our node
						Node sendingNode = new Node(packet.getAddress().getHostAddress(),Application.UDP_FD_PORT);
						if(Application.verbose)
							System.out.println("Received election message from: "+sendingNode.getIP());
						Node self = null;
						synchronized(Application.getInstance().group)
						{
							self = Application.getInstance().group.getSelfNode();
						}
						if(self.compareTo(sendingNode) > 0)
						{
							// send reply
							InetAddress target = null;
							try {
								target = InetAddress.getByName(sendingNode.getIP());
							} catch (UnknownHostException e) {
								e.printStackTrace();
							}
							
							try {
								sendData(target,"ANSWER");
							} catch (IOException e) {
								e.printStackTrace();
							}
						} else if(self.compareTo(sendingNode) < 0)
						{
							// dont send reply
						} else {
							Application.LOGGER.warning("FailureDetectorServer - producer.run() - received election message from self.");
						}
					} else if(data.equals("ANSWER")) {
						// mark that an answer message has been received
						FailureDetectorClient.receivedAnswerMessage(new Date().getTime());
						
						String sendingNodeIP = packet.getAddress().getHostAddress();
						if(Application.verbose)
							System.out.println("Received answer message from: "+sendingNodeIP);
						Application.LOGGER.info("FailureDetectorServer - producer.run() - received answer message from: "+sendingNodeIP);
					} else if(data.equals("COORDINATOR")) {
						
						// new leader is elected
						String sendingNodeIP = packet.getAddress().getHostAddress();
						if(Application.verbose)
							System.out.println("Received coordinator message from: "+sendingNodeIP);
						boolean result = Application.getInstance().group.receivedCoordinatorMessage(new Node(sendingNodeIP,Application.UDP_FD_PORT));
						synchronized(FailureDetectorClient.leaderElection)
						{
							FailureDetectorClient.resetLeaderElectionStatus();
						}
						if(result)
						{
							Application.LOGGER.info("FailureDetectorServer - producer.run() - received coordinator message from: "+sendingNodeIP);
						} else {
							Application.LOGGER.warning("FailureDetectorServer - producer.run() - received coordinator message from node not in list!: "+sendingNodeIP);
						}
					}
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		
	    private void sendData(InetAddress target, String data) throws IOException{
	    	DatagramSocket datagramSocket = null;
			// get a datagram socket
			try {
				datagramSocket = new DatagramSocket();
			} catch (SocketException e) {
				e.printStackTrace();
			}
    		if(!FailureDetectorClient.isRandomFailure())
    		{
	    		// generate random number to determine whether or  not to drop the packet
		    	if(data.getBytes("utf-8").length > 256) {
		    		Application.LOGGER.info("FailureDetectorServer - sendData - string data is too long");
		    		throw new IOException("string data is too long");
		    	}
		        DatagramPacket datagram = new DatagramPacket(data.getBytes("utf-8"), data.length(), target, Application.UDP_FD_PORT);
		        datagramSocket.send(datagram);
				Application.LOGGER.info(new Date().getTime() +" FailureDetectorServer - producer.run() - Sending message: "+data);
    		} else {
				Application.LOGGER.info(new Date().getTime() +" FailureDetectorServer - producer.run() - \""+data+"\" message dropped!");
    		}
	    }
		
	}
	
}
