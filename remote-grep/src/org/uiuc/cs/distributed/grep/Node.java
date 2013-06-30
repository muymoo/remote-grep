package org.uiuc.cs.distributed.grep;

import java.net.UnknownHostException;

/**
 * This is a class representation of a grep node. This class could be used to represent a server for which to grep.
 * 
 * @author evan
 */
public class Node implements Comparable<Node>
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

    @Override
    public int compareTo(Node otherNode)
    {
    	return Long.valueOf(this.getTimestamp()).compareTo(Long.valueOf(otherNode.getTimestamp()));
    }

    @Override
    public boolean equals(Object object)
    {
        boolean result = false;

        if (object != null && object instanceof Node)
        {
            result = this.ip.equals(((Node) object).ip);
        }

        return result;
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
    
    public boolean isSelf(String selfIpAddress)
    {
    	if(this.ip.equals(selfIpAddress))
    	{
    		return true;
    	} else {
    		return false;
    	}
    }
    
    @Override
    public String toString()
    {
        return this.timestamp + ":" + this.ip + ":" + this.port;
    }
}
