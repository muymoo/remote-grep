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
    private String ip="";
    private int    port=0;
    public long lastUpdatedTimestamp = -1;
    private boolean valid = false;
    

    public Node(String ipAndPort)
    {
        String[] parts = ipAndPort.split(":");
        if ( parts.length == 2 )
        {
            this.ip = parts[0];
            this.port = Integer.parseInt(parts[1]);
        }
        init();
    }

    public Node(String _ip, int _port) throws IllegalArgumentException
    {
		this(new Date().getTime(), _ip, _port);
		init();
    }
    
    public Node(long _timestamp, String _ip, int _port)
    {
    	this.timestamp = _timestamp;
    	this.ip = _ip;
    	this.port = _port;
    	init();
    }
    
    public Node(long _timestamp, String _ip, int _port, long _lastUpdatedTimestamp)
    {
    	this.timestamp = _timestamp;
    	this.ip = _ip;
    	this.port = _port;
    	this.lastUpdatedTimestamp = _lastUpdatedTimestamp;
    	init();
    }
    
    /**
     * init must be called within the constructor to set the valid value
     */
    private void init()
    {
    	valid = isValidIP();
    }

    /**
     * this overrides the compareTo function so that Node object can be sorted 
     * according to the IP address
     */
    @Override
    public int compareTo(Node otherNode)
    {
    	String ipParts[] = this.ip.split("[.]");
    	String otherIpParts[] = otherNode.getIP().split("[.]");
    	

    	// compare each ip component
    	for(int i=0;i<ipParts.length;i++)
    	{
    		try {
    			int currComparison = Integer.valueOf(ipParts[i]).compareTo(Integer.valueOf(otherIpParts[i]));
    			if(currComparison != 0)
    			{
    				return currComparison;
    			}
        		
    		} catch(NumberFormatException e) {
    		}
    	}
    	return 0;
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
        return valid;
    }
    
    private boolean isValidIP()
    {
    	String ipParts[] = this.ip.split("[.]");
    	// make sure there are at least 4 parts separated by "."
    	if(ipParts.length != 4)
    	{
    		return false;
    	}
    	// check that each ip component is in the range [0,255]
    	for(int i=0;i<ipParts.length;i++)
    	{
    		try {
    			int currInt = Integer.parseInt(ipParts[i]);
        		if(currInt < 0 || currInt > 256)
        		{
        			return false;
        		}
    		} catch(NumberFormatException e) {
    			return false;
    		}
    	}
    	return true;
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
    
    public static void main(String[] args)
    {
    	GroupMembership groupMembership = new GroupMembership("1.2.3.6");
    	groupMembership.add(new Node("1.2.3.4:50"));
    	groupMembership.add(new Node("1.2.3.5:50"));
    	groupMembership.add(new Node("1.2.3.6:50"));
        Node receiveNode = groupMembership.getHeartbeatReceiveNode();
        
        boolean expected = true;
        boolean actual = receiveNode.getIP() == "1.2.3.4";
        System.out.println("hello");
    }
}
