package org.uiuc.cs.distributed.grep;

import java.util.Date;

/**
 * This is a class representation of a grep node. This class could be used to represent a server for which to grep.
 * 
 * @author evan
 */
public class Node implements Comparable<Node>
{
	private long timestamp;
    private String ip;
    private int    port;
    public long lastUpdatedTimestamp = -1;
    

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
		this(new Date().getTime(), _ip, _port);
    }
    
    public Node(long _timestamp, String _ip, int _port)
    {
    	this.timestamp = _timestamp;
    	this.ip = _ip;
    	this.port = _port;
    }
    
    public Node(long _timestamp, String _ip, int _port, long _lastUpdatedTimestamp)
    {
    	this.timestamp = _timestamp;
    	this.ip = _ip;
    	this.port = _port;
    	this.lastUpdatedTimestamp = _lastUpdatedTimestamp;
    }

    @Override
    public int compareTo(Node otherNode)
    {
    	return Long.valueOf(this.getTimestamp()).compareTo(Long.valueOf(otherNode.getTimestamp()));
    }
    
    public int lastUpdatedCompareTo(Node otherNode)
    {
    	return Long.valueOf(this.lastUpdatedTimestamp).compareTo(Long.valueOf(otherNode.lastUpdatedTimestamp));
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

    public long getTimestamp()
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
    
    public String verboseToString()
    {
    	return "<Node "+this.timestamp + ":" + this.ip + ":" + this.port + ", lastUpdated: " +this.lastUpdatedTimestamp+">";
    }
}
