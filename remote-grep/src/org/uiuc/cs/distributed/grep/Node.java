package org.uiuc.cs.distributed.grep;

/**
 * This is a class representation of a grep node. This class could be used to represent a server for which to grep.
 * 
 * @author evan
 */
public class Node
{
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
        this.ip = _ip;
        this.port = _port;
    }

    public String getIP()
    {
        return this.ip;
    }

    public int getPort()
    {
        return this.port;
    }

    public boolean isValid()
    {
        return false;
    }

    @Override
    public String toString()
    {
        return this.ip + ":" + this.port;
    }
}
