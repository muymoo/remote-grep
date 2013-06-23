package org.uiuc.cs.distributed.grep;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class GroupServerThread extends Thread {
	protected DatagramSocket socket = null;
	protected BufferedReader in = null;
	protected boolean moreQuotes = true;
	private RemoteGrepApplication app = RemoteGrepApplication.getInstance();
	
	public GroupServerThread() {
		this("GroupServerThread");
	}

	public GroupServerThread(String name)  {
		super(name);
        
		try {
			socket = new DatagramSocket(4445);
		} catch (SocketException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		try {
			in = new BufferedReader(new FileReader("one-liners.txt"));
		} catch (FileNotFoundException e) {
			System.err
					.println("Couldn't open quote file.  Serving time instead.");
		}
	}

	public void run() {

		while (moreQuotes) {
			try {
				byte[] buf = new byte[256];

				// receive request
				DatagramPacket packet = new DatagramPacket(buf, buf.length);
				socket.receive(packet);

				// Add node to group list
				app.groupMemebershipList.add(in.readLine());
				
				// figure out response
//				String dString = null;
//				if (in == null)
//					dString = new Date().toString();
//				else
//					dString = getNextQuote();
//
//				buf = dString.getBytes();

				// send the response to the client at "address" and "port"
				InetAddress address = packet.getAddress();
				int port = packet.getPort();
				packet = new DatagramPacket(buf, buf.length, address, port);
				socket.send(packet);
			} catch (IOException e) {
				e.printStackTrace();
				moreQuotes = false;
			}
		}
		socket.close();
	}

//	protected String getNextQuote() {
//		String returnValue = null;
//		try {
//			if ((returnValue = in.readLine()) == null) {
//				in.close();
//				moreQuotes = false;
//				returnValue = "No more quotes. Goodbye.";
//			}
//		} catch (IOException e) {
//			returnValue = "IOException occurred in server.";
//		}
//		return returnValue;
//	}
}
