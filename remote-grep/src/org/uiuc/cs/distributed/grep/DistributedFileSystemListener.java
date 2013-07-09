package org.uiuc.cs.distributed.grep;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class DistributedFileSystemListener extends Thread {
    
    private ServerSocket   serverSocket = null;
    private int            serverPort   = Application.TCP_SDFS_PORT;
    private boolean        listening    = true;
    private boolean        foundPort;
	
	/**
     * Names the thread 
     */
    public DistributedFileSystemListener()
    {
        super("DistributedFileSystemListenerThread");
        this.foundPort = false;
    }

    /**
     * Listen for incoming put, get, delete requests to this machine.
     */
    @Override
    public void run()
    {
        try
        {
            int numTriesLeft = 1000;
            while (!this.foundPort && numTriesLeft > 0)
            {
                try
                {
                    this.serverSocket = new ServerSocket(serverPort);
                    this.foundPort = true;
                }
                catch (IOException e)
                {
                    this.serverPort++; // try the next port number
                    numTriesLeft--;
                }
            }
            if ( !this.foundPort )
            {
                Application.LOGGER.severe("SDFSListener - run - Could not listen on port: " + serverPort);
                System.exit(-1);
            }
            Application.LOGGER.info("SDFSListener - run - Server started on socket: " + serverPort);

            while (listening)
            {
                Socket clientSocket = serverSocket.accept();
                Application.LOGGER.info("SDFSListener - run - accepted connection from: " + clientSocket.getInetAddress() + ":"
								+ clientSocket.getPort());

				// Setup our input/output streams
				byte[] buffer = new byte[65536];
				int number;
				InputStream socketStream = clientSocket.getInputStream();
				File f = new File("/tmp/output.dat");
				OutputStream fileStream = new FileOutputStream(f);
				
				// Read file from sender
				while ((number = socketStream.read(buffer)) != -1) {
					fileStream.write(buffer, 0, number);
				}

				fileStream.close();
                socketStream.close();
                clientSocket.close();

            }

            serverSocket.close();
            Application.LOGGER.info("SDFSListener - run - socket closed, shutting down server.");
        }
        catch (IOException e)
        {
            Application.LOGGER.info("SDFSListener - run - IOException: " + e.getMessage() + " stack trace: "
                    + e.getStackTrace().toString());
        }
    }
}
