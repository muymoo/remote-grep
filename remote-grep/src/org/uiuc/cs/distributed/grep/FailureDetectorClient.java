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
	public static double messageFailureRate;
	public static LeaderElection leaderElection = new LeaderElection();
	
	public static class LeaderElection {
	
		// Leader Election variables
		/**
		 * The Failure Detector Client acts to elect a leader should 
		 * the leader node fail using the Bully Election Algorithm.
		 * This works on synchronous systems and uses the global timeout
		 * variable : Application.timeBoundedFailureInMilli to
		 * determine whether or not it should announce a coordinator message
		 * and declare itself the new leader.
		 */
		private boolean sentElectionMessages = false;
		private long timeElectionMessagesSent = 0;
		private boolean receivedAnswerMessages = false;
		private long timeReceivedAnswerMessage = 0;
	}
	
	public FailureDetectorClient() {
		try {
			hostaddress = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e1) {
			e1.printStackTrace();
		}
		// get a datagram socket
		try {
			socket = new DatagramSocket();
		} catch (SocketException e) {
			e.printStackTrace();
		}
		resetLeaderElectionStatus();

		client = new Thread(new FailureDetectorClientRunnable(),"FailureDetectorClient");
		client.start();
	}
	
	public static synchronized void resetLeaderElectionStatus()
	{
		FailureDetectorClient.leaderElection.sentElectionMessages = false;
		FailureDetectorClient.leaderElection.timeElectionMessagesSent = 0;
		FailureDetectorClient.leaderElection.receivedAnswerMessages = false;
		FailureDetectorClient.leaderElection.timeReceivedAnswerMessage = 0;
	}
	
	public static synchronized void receivedAnswerMessage(long timestamp)
	{
		synchronized(FailureDetectorClient.leaderElection)
		{
			FailureDetectorClient.leaderElection.receivedAnswerMessages = true;
			FailureDetectorClient.leaderElection.timeReceivedAnswerMessage = timestamp;
		}
	}
	
	public static boolean isRandomFailure()
	{
		if(messageFailureRate == 0.0)
		{
			return false;
		} else {
			double rand = Math.random();
			System.out.println("rand: "+rand);
			if(Math.random() < messageFailureRate)
			{
				return true;
			} else {
				return false;
			}
		}
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

		public FailureDetectorClientRunnable()
		{
		}
		
	
		public void run()
		{
			while(!Thread.currentThread().isInterrupted()) {

				synchronized(Application.getInstance().group.list)
				{
					// No heartbeats to send
					if(Application.getInstance().group.list.size() < 2)
					{
						continue;
					}
				}
				
				boolean electionInProgress = false;
				synchronized(Application.getInstance().group.list)
				{
					electionInProgress = Application.getInstance().group.electionInProgress;
				}
				
				
				if(electionInProgress && Application.getInstance().groupServer.joinedGroup)
				{
					boolean sendElection = false;
					boolean sendCoordinator = false;
					synchronized(FailureDetectorClient.leaderElection)
					{
						if(!FailureDetectorClient.leaderElection.sentElectionMessages)
						{
							sendElection = true;
							
						} else {
							if(FailureDetectorClient.leaderElection.receivedAnswerMessages)
							{
								long timeSinceReceived = new Date().getTime() - FailureDetectorClient.leaderElection.timeReceivedAnswerMessage; 
								
								if(timeSinceReceived > (5 * Application.timeBoundedFailureInMilli))
								{
									Application.LOGGER.warning("FailureDetectorClient - run() - has not received coordinator message in over 5 * timeout!!");
								}
							} else {
								long timeSinceReceived = new Date().getTime() - FailureDetectorClient.leaderElection.timeElectionMessagesSent; 
								if(timeSinceReceived > (2 * Application.timeBoundedFailureInMilli))
								{
									Application.LOGGER.warning("FailureDetectorClient - run() - determined we are the leader");
									System.out.println("Determined we are the leader");
									sendCoordinator = true;
								}
							}
						}
					}
					
					if(sendElection && sendCoordinator)
					{
						Application.LOGGER.warning("both election and coordinator messages are being sent");
					}
					if(sendElection)
					{
						// send election messages to all other nodes
						List<Node> otherNodes = Application.getInstance().group.getOtherNodes();
						for(int nodeIndex = 0; nodeIndex < otherNodes.size();nodeIndex++)
						{
							Node node = otherNodes.get(nodeIndex);
							InetAddress target = null;
							try {
								target = InetAddress.getByName(node.getIP());
							} catch (UnknownHostException e) {
								e.printStackTrace();
							}
							
							try {
								sendData(target,"ELECTION");
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
						synchronized(FailureDetectorClient.leaderElection)
						{
							FailureDetectorClient.leaderElection.sentElectionMessages = true;
							FailureDetectorClient.leaderElection.timeElectionMessagesSent = new Date().getTime();
						}
					}
					if(sendCoordinator)
					{
						// send election messages to all upper nodes
						List<Node> lowerNodes = Application.getInstance().group.getLowerMembers();
						for(int nodeIndex = 0; nodeIndex < lowerNodes.size();nodeIndex++)
						{
							Node node = lowerNodes.get(nodeIndex);
							InetAddress target = null;
							try {
								target = InetAddress.getByName(node.getIP());
							} catch (UnknownHostException e) {
								e.printStackTrace();
							}
							
							try {
								sendData(target,"COORDINATOR");
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
						
						synchronized(Application.getInstance().group)
						{
							Application.getInstance().group.setSelfAsLeader();
						}
						synchronized(FailureDetectorClient.leaderElection)
						{
							FailureDetectorClient.resetLeaderElectionStatus();
						}
					}
				} else if(Application.getInstance().groupServer.joinedGroup) {
					// Only send heartbeats if an election is not in progress
					int nodesContacted = 0;
					Node node = null;
					
					synchronized(Application.getInstance().group.list)
					{	
						node = Application.getInstance().group.getHeartbeatSendNode();
					}
					
					if(node != null)
					{
						nodesContacted++;				
						
						InetAddress target = null;
						try {
							target = InetAddress.getByName(node.getIP());
						} catch (UnknownHostException e) {
							e.printStackTrace();
						}
						
						try {
							System.out.println("Sending heartbeat to :"+node.getIP());
							sendData(target,"HEARTBEAT");
						} catch (IOException e) {
							e.printStackTrace();
						}
					}

					if(nodesContacted != 1) {
						Application.LOGGER.info(new Date().getTime()+" FailureDetectorClient - run() - did not send heartbeat message to peer");
					}
				}
				
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
			}
		}
		
	    private void sendData(InetAddress target, String data) throws IOException{
    		if(!isRandomFailure())
    		{
	    		// generate random number to determine whether or  not to drop the packet
		    	if(data.getBytes("utf-8").length > 256) {
		    		Application.LOGGER.info("FailureDetectorClient - sendData - string data is too long");
		    		throw new IOException("string data is too long");
		    	}
		        DatagramPacket datagram = new DatagramPacket(data.getBytes("utf-8"), data.length(), target, Application.UDP_FD_PORT);
		        socket.send(datagram);
				Application.LOGGER.info(new Date().getTime() +" FailureDetectorClient - run() - Sending message: "+data);
    		} else {
				Application.LOGGER.info(new Date().getTime() +" FailureDetectorClient - run() - \""+data+"\" message dropped!");
    		}
	    }
	}
}
