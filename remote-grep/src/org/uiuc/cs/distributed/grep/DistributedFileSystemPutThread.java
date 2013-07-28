package org.uiuc.cs.distributed.grep;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class DistributedFileSystemPutThread extends Thread {
	private Socket clientSocket;
	private String key;

	public DistributedFileSystemPutThread(Socket socket, String sdfsKey) {
		super("PutThread");
		clientSocket = socket;
		key = sdfsKey;
	}

	@Override
	public void run() {
		// We are going to store the put'd file on the local machine here
		// (unique file name)
		String fileName = Application.getInstance().dfsClient
				.generateNewFileName("FILLER.data");
		File localFile = new File(fileName);

		byte[] buffer = new byte[65536];
		int number;
		InputStream socketStream;
		try {
			socketStream = clientSocket.getInputStream();

			OutputStream fileStream = new FileOutputStream(localFile);

			// Read file from sender
			while ((number = socketStream.read(buffer)) != -1) {
				fileStream.write(buffer, 0, number);
			}

			fileStream.close();
			socketStream.close();
			clientSocket.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
		// Update the file map on this node
		Application.getInstance().dfsClient.updateFileMap(key, fileName);
	}

}
