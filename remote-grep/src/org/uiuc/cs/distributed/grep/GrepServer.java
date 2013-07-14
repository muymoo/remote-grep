package org.uiuc.cs.distributed.grep;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * The grep server listens for regular expressions (regex) coming into port 4444
 * and returns the results of the grep command using the given regex on the
 * machine.1.log file.
 * 
 * @author matt
 * @author evan
 * 
 */
public class GrepServer extends Thread
{
    private ServerSocket   serverSocket = null;
    private int            serverPort   = Application.TCP_PORT;
    private boolean        listening    = true;
    private boolean        foundPort;

    /**
     * Names the thread and sets up the default log file location.
     */
    public GrepServer()
    {
        super("GrepServerThread");
        this.foundPort = false;
    }

    public synchronized int getPort()
    {
        return this.serverPort;
    }

    /**
     * Listens for a client to request a grep command then runs the grep command on the server. The results are returned over the socket connection to the
     * client.
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
                Application.LOGGER.severe("GrepServer - run - Could not listen on port: " + serverPort);
                System.exit(-1);
            }
            Application.LOGGER.info("GrepServer - run - Server started on socket: " + serverPort);

            while (listening)
            {
                Socket clientSocket = serverSocket.accept();
                Application.LOGGER.info("GrepServer - run - accepted connection from: " + clientSocket.getInetAddress() + ":"
                        + clientSocket.getPort());

                // Setup our input/output streams
                PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

                String clientInput, clientOutput;
                Grep grep = new Grep();

                // Loop until grep doesn't return any more results
                while ((clientInput = in.readLine()) != null)
                {
                    Application.LOGGER.info("GrepServer - run - clientInput: " + clientInput);
                    if ( clientInput.equals("<QUIT>") )
                    {
                        listening = false;
                        out.println("Shutting Down");
                        break;
                    }
                    	clientOutput = grep.search(clientInput); // run grep
                    Application.LOGGER.info("GrepServer - run - clientOutput: " + clientOutput);
                    clientOutput += "<END>\n";
                    out.print(clientOutput); // Send results back to client
                    out.println("<END>");
                }

                out.close();
                in.close();
                clientSocket.close();

            }

            serverSocket.close();
            Application.LOGGER.info("GrepServer - run - socket closed, shutting down server.");
        }
        catch (IOException e)
        {
            Application.LOGGER.info("GrepServer - run - IOException: " + e.getMessage() + " stack trace: "
                    + e.getStackTrace().toString());
        }
    }
}
