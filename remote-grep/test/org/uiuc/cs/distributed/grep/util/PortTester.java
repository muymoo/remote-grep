package org.uiuc.cs.distributed.grep.util;

import java.net.BindException;
import java.net.DatagramSocket;
import java.net.Socket;
import java.net.SocketException;

import org.uiuc.cs.distributed.grep.Application;

public class PortTester {

	private static int initial_port = 4448;
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		
		
		for(int i=0;i<1000;i++)
		{
			int curr_port = initial_port + i;
			try {
				DatagramSocket socket =new DatagramSocket(initial_port);
				socket.close();
				System.out.println("UDP open: "+curr_port);
			} catch (BindException e) {
				
			} catch (SocketException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

}
