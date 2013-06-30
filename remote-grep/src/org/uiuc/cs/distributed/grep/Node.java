package org.uiuc.cs.distributed.grep;

/**
 * This is a class representation of a grep node. This class could be used to represent a server for which to grep.
 * 
 * @author evan
 */
public class Node
{
	private String timestamp = "";
    private String ip;
    private int    port;

    public Node(String ipAndPort)
    {
        String[] parts = ipAndPort.split(":");
        if ( parts.length == 2 )
        {
            this.ip = parts[0];
            this.port = Integer.parseInt(parts[1]);
        }
    }

    public Node(String _ip, int _port) throws IllegalArgumentException
    {
		this("", _ip, _port);
    }
    
    public Node(String timestamp, String ip, int port)
    {
    	this.timestamp = timestamp;
    	this.ip = ip;
    	this.port = port;
    }

    public String getIP()
    {
        return this.ip;
    }

    public int getPort()
    {
        return this.port;
    }
    
    public String getTimestamp()
    {
    	return this.timestamp;
    }

    public boolean isValid()
    {
        return false;
    }

    @Override
    public String toString()
    {
        return this.timestamp + ":" + this.ip + ":" + this.port;
    }
    
    @Override
    public boolean equals(Object object)
    {
        boolean sameSame = false;

        if (object != null && object instanceof Node)
        {
            sameSame = this.ip == ((Node) object).ip;
        }

        return sameSame;
    }
}
