package org.uiuc.cs.distributed.grep;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.UUID;

public class DistributedFileSystemGetThread extends Thread {
	private String serverIP;
	private int serverPort;
	private String key;

	public DistributedFileSystemGetThread(String _serverIP, int _serverPort, String sdfsKey) {
		super("GetThread");
		serverIP = _serverIP;
		serverPort = _serverPort;
		key = sdfsKey;
	}

	@Override
	public void run() {
		Socket socket = null;
		try {
			socket = new Socket(serverIP, serverPort);
		} catch (UnknownHostException e3) {
			e3.printStackTrace();
		} catch (IOException e3) {
			e3.printStackTrace();
		}
		byte[] buffer = new byte[65536];
		int number;
		System.out.println("key: " + key + " IP: "+serverIP);
		String localFile = Application.getInstance().dfsClient.getFileLocation(key);
		
		OutputStream socketOutputStream = null;
		try {
			socketOutputStream = socket.getOutputStream();
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		
		if(localFile == null)
		{
			localFile = Application.SDFS_DIR+File.separator+Application.hostaddress+"_"+ UUID.randomUUID().toString() + ".data";
		}
		
        File sdfsFile =  new File(localFile); // get the sdfs file from local storage
		if (!sdfsFile.exists()) {
			try {
				sdfsFile.createNewFile();
			} catch (IOException e) {
				System.out
						.println("Could not create destination file. Do you have permission?");
				e.printStackTrace();
				return;
			}
		}
		
		InputStream fileInputStream = null;
		try {
			fileInputStream = new FileInputStream(sdfsFile);
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		
		
        try {
            System.out.println("reading in file: " + fileInputStream);
            while ((number = fileInputStream.read(buffer)) != -1) {
                    try {
                            socketOutputStream.write(buffer, 0, number);
                    } catch (IOException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                    }
            }
        } catch (IOException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
        }

        try {
                socketOutputStream.close();
                fileInputStream.close();
                socket.close();
        } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
        } 
	}

}
