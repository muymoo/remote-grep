package org.uiuc.cs.distributed.grep;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class DistributedFileSystemPutThread extends Thread {
	private String serverIP;
	private int serverPort;
	private String key;

	public DistributedFileSystemPutThread(String _serverIP, int _serverPort, String sdfsKey) {
		super("PutThread");
		serverIP = _serverIP;
		serverPort = _serverPort;
		key = sdfsKey;
	}

	@Override
	public void run() {
		// We are going to store the put'd file on the local machine here
		// (unique file name)
		String fileName = Application.getInstance().dfsClient
				.generateNewFileName("FILLER.data");
		File localFile = new File(fileName);
		Application.LOGGER.info("DistributedFileSystemPutThread - run - putting file: "+fileName);

		byte[] buffer = new byte[65536];
		int number;
		InputStream socketStream;
		try {
			Socket socket = new Socket(serverIP, serverPort);
			socketStream = socket.getInputStream();

			OutputStream fileStream = new FileOutputStream(localFile);

			// Read file from sender
			int writeCounts = 0;
			while ((number = socketStream.read(buffer)) != -1) {
				fileStream.write(buffer, 0, number);
				writeCounts++;
			}
			Application.LOGGER.info("DistributedFileSystemPutThread - run - put "+writeCounts+" writes for file: "+fileName);

			fileStream.close();
			socketStream.close();
			socket.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
		// Update the file map on this node
		Application.getInstance().dfsClient.updateFileMap(key, fileName);
	}

}
