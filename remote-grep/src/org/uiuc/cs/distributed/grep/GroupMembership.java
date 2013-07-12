package org.uiuc.cs.distributed.grep;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GroupMembership {
	
	public List<Node> list;
	
	private int selfIndex = 0;
	private String selfIP;
	
	
	public GroupMembership(String _selfIP) {
		this.list = Collections
				.synchronizedList(new ArrayList<Node>());
		this.selfIP = _selfIP;
	}
	
	public List<Node> getUpperMembers()
	{
		if(this.list.size() < 2)
		{
			return null;
		}
		
		List<Node> list = new ArrayList<Node>();
		for(int i=this.selfIndex;i<this.list.size();i++)
		{
			if(i != this.selfIndex)
			{
				list.add(this.list.get(i));
			}
		}
		return list;
	}
	
	public List<Node> getLowerMembers()
	{
		if(this.list.size() < 2)
		{
			return null;
		}
		
		List<Node> list = new ArrayList<Node>();
		for(int i=0;i<this.selfIndex;i++)
		{
			if(i != this.selfIndex)
			{
				list.add(this.list.get(i));
			}
		}
		return list;
	}
	
	/**
	 * return the node that should be receive heartbeat messages from this node.
	 * It should be the node ranked below this node.
	 * 
	 * @return Node - the node to send heartbeat messages to
	 */
	public Node getHeartbeatSendNode()
	{
		if(this.list.size() < 2)
		{
			return null;
		}
		
		if(this.selfIndex == 0)
		{
			return this.list.get(this.list.size() - 1); // return the last node
		}
		return this.list.get(this.selfIndex - 1);
	}
	
	/**
	 * return the node that should be sending heartbeat messages to this node.
	 * It should be the node ranked above this node. 
	 * 
	 * @return Node - the node to expect to receive heartbeat messages from
	 */
	public Node getHeartbeatReceiveNode()
	{
		if(this.list.size() < 2)
		{
			return null;
		}
		
		return this.list.get((this.selfIndex + 1) % this.list.size());
	}
	
	/**
	 * returns the index of the node which corresponds to itself
	 * this is used to determine which node to send to(self -1) and 
	 * receive (self +1) heart beat messages. This should be called only
	 * each time the list itself is changed.
	 * 
	 * @return int - the index of the self node 
	 */
	public int getSelfIndex()
	{
		if(this.list.size() == 0)
		{
			return -1;
		} else {
			for(int i=0;i<this.list.size();i++)
			{
				if(this.list.get(i).getIP().compareTo(this.selfIP) == 0)
				{
					return i;
				}
			}
		}
		return -1;
	}
	
	public void add(Node node)
	{
		if(node.isValid() &&
				!this.list.contains(node))
		{
			this.list.add(node);
			Collections.sort(this.list);
			this.selfIndex = getSelfIndex();
		}
	}
	
	public void remove(Node node)
	{
		if(this.list.contains(node))
		{
			this.list.remove(node);
			this.selfIndex = getSelfIndex();
		}
	}
}
