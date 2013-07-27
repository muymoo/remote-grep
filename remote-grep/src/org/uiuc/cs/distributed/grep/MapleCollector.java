package org.uiuc.cs.distributed.grep;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * Listens for incoming maple responses and then starts a new thread to handle
 * the response. This class allows us to have multiple maple clients replying at
 * the same time on the same port.
 * http://docs.oracle.com/javase/tutorial/networking/sockets/clientServer.html
 * 
 * @author matt
 * 
 */
public class MapleCollector extends Thread {
	@Override
	public void run() {
		ServerSocket serverSocket = null;
		boolean listening = true;
		try {
			serverSocket = new ServerSocket(Application.TCP_MAPLE_PORT);
		} catch (IOException e) {
			System.err.println("Could not listen on port: "
					+ Application.TCP_MAPLE_PORT);
			return;
		}

		// For each incoming response, start a new thread to handle the
		try {
			while (listening) {
				new MapleCollectorThread(serverSocket.accept()).start();
			}
			serverSocket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}