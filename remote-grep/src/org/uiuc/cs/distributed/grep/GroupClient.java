package org.uiuc.cs.distributed.grep;

import java.io.File;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class GroupClient extends Thread {
	private Node node;
    private static Logger  LOGGER;
    private static Handler logFileHandler;
    
	GroupClient(Node node) {
		this.node = node;
        String logFileLocation = RemoteGrepApplication.logLocation + File.separator + "logs" + File.separator
                + "groupclient.log";
        try
        {
            logFileHandler = new FileHandler(logFileLocation);
        }
        catch (SecurityException e)
        {
            e.printStackTrace();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        
        LOGGER = Logger.getLogger("GrepServer");
        LOGGER.setUseParentHandlers(false);
        logFileHandler.setFormatter(new SimpleFormatter());
        logFileHandler.setLevel(Level.INFO);
        LOGGER.addHandler(logFileHandler);
	}

	// Asks to join LINUX_7 group and listens for updates 
	public void run() {

		// get a datagram socket
		DatagramSocket socket = null;
		try {
			socket = new DatagramSocket();
		} catch (SocketException e) {
			e.printStackTrace();
		}

		// send request
		byte[] buf = new byte[256];
		
		// So we can uniquely identify this node (we can get IP/port from packet by default)
		String joinRequest = String.valueOf(System.currentTimeMillis());
		LOGGER.info("Join request: " + joinRequest);
		buf = joinRequest.getBytes();
		LOGGER.info("Buffer: " + buf);
		LOGGER.info("Buffer.toString(): " + buf.toString());
		
		InetAddress address = null;
		try {
			address = InetAddress.getByName(node.getIP());
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
		DatagramPacket packet = new DatagramPacket(buf, buf.length, address,
				4445);
		try {
			socket.send(packet);
		} catch (IOException e) {
			e.printStackTrace();
		}

		// get response
//		packet = new DatagramPacket(buf, buf.length);
//		try {
//			socket.receive(packet);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//
//		// display response
//		String received = new String(packet.getData(), 0, packet.getLength());

		socket.close();
	}
}