package org.uiuc.cs.distributed.grep;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

/**
 * Recieves maple input from other nodes
 * 
 * @author matt
 * 
 */
public class MapleCollectorThread extends Thread {
	private Socket socket;

	public MapleCollectorThread(Socket clientSocket) {
		super("MapleCollectorThread");
		this.socket = clientSocket;
	}

	@Override
	public void run() {
		try {
			BufferedReader in = new BufferedReader(new InputStreamReader(
					socket.getInputStream()));

			String inputLine;

			while ((inputLine = in.readLine()) != "<END>") {
				System.out.println("Maple received: " + inputLine);
				// TODO: Do stuff with maple response. Maybe combine files.
			}
			in.close();
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
