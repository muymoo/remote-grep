package org.uiuc.cs.distributed.grep;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GroupMembership {
	
	public List<Node> list;
	
	private int selfIndex = 0;
	private String selfIP;
	private String leaderIP;
	private int leaderIndex = 0;
	public boolean electionInProgress = true;
	
	
	public GroupMembership(String _selfIP) {
		this.list = Collections
				.synchronizedList(new ArrayList<Node>());
		this.selfIP = _selfIP;
	}
	
	/**
	 * get the nodes that ranked higher than this node
	 * 
	 * @return
	 */
	public List<Node> getUpperMembers()
	{
		List<Node> list = new ArrayList<Node>();
		if(this.list.size() < 2)
		{
			return list;
		}

		for(int i=this.selfIndex;i<this.list.size();i++)
		{
			if(i != this.selfIndex)
			{
				list.add(this.list.get(i));
			}
		}
		return list;
	}
	
	public void resetLastUpdated()
	{
		for(int i=0;i<this.list.size();i++)
		{
			this.list.get(i).lastUpdatedTimestamp = -1;
		}
	}
	
	/**
	 * get the nodes that are ranked lower than this node
	 * 
	 * @return List<Node> - list of lower ranked nodes
	 */
	public List<Node> getLowerMembers()
	{
		List<Node> list = new ArrayList<Node>();
		if(this.list.size() < 2)
		{
			return list;
		}
		
		for(int i=0;i<this.selfIndex;i++)
		{
			if(i != this.selfIndex)
			{
				list.add(this.list.get(i));
			}
		}
		return list;
	}
	
	public Node getLeader()
	{
		/*
		for(int i=0;i<this.list.size();i++) {
			if(this.list.get(i).getIP().equals(Application.INTRODUCER_IP))
			{
				return this.list.get(i);
			}
		}
		return null;
*/
		
		if(!this.electionInProgress)
		{
			return this.list.get(leaderIndex);
		}
		return null;
		
	}
	
	public boolean isLeader()
	{
		/*if(Application.hostaddress.equals(Application.INTRODUCER_IP))
		{
			return true;
		}
		return false;
		*/
		if(!this.electionInProgress && 
			this.getSelfIndex() == this.leaderIndex)
		{
			return true;
		}
		return false;
	}
	
	public void setSelfAsLeader()
	{
		this.leaderIndex = this.selfIndex;
		this.electionInProgress = false;
	}

	
	/**
	 * return a list of all nodes except this one
	 * 
	 * @return List<Node> - all other nodes
	 */
	public List<Node> getOtherNodes()
	{
		List<Node> list = new ArrayList<Node>();
		if(this.list.size() < 2)
		{
			return list;
		}

		for(int i=0;i<this.list.size();i++)
		{
			if(i != this.selfIndex)
			{
				list.add(this.list.get(i));
			}
		}
		return list;
	}
	
	public boolean receivedCoordinatorMessage(Node node)
	{
		for(int i=0;i<this.list.size();i++)
		{
			if(node.getIP().equals(this.list.get(i).getIP()))
			{
				this.leaderIndex = i;
				this.electionInProgress = false;
				return true;
			}
		}
		return false;
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
			return 0;
		} else {
			for(int i=0;i<this.list.size();i++)
			{
				if(this.list.get(i).getIP().compareTo(this.selfIP) == 0)
				{
					return i;
				}
			}
		}
		return 0;
	}

	
	/**
	 * return the node that corresponds to the machine this process
	 * is running on
	 * 
	 * @return Node - self node
	 */
	public Node getSelfNode()
	{
		if(this.list.size() == 0)
		{
			return null;
		} else {
			return this.list.get(this.selfIndex);
		}
	}
	
	public void add(Node node)
	{
		if(node.isValid() &&
				!this.list.contains(node))
		{
			Node leaderNode = null;
			if(!this.electionInProgress && this.list.size() > 1)
				leaderNode = getLeader();
			
			this.list.add(node);
			Collections.sort(this.list);
			this.selfIndex = getSelfIndex();
			
			
			if(leaderNode != null)
			{
				for(int i=0;i<this.list.size();i++)
				{
					if(this.list.get(i).getIP().equals(leaderNode.getIP()))
					{
						this.leaderIndex = i;
					}
				}
			}
		}
	}
	
	public void remove(Node node)
	{
		if(this.list.contains(node))
		{
			Node leaderNode = getLeader();
			if(leaderNode != null)
			{
				if(node.getIP().equals(leaderNode.getIP()))
				{
					this.electionInProgress = true;
				}
				for(int i=0;i<this.list.size();i++)
				{
					if(this.list.get(i).getIP().equals(leaderNode.getIP()))
					{
						this.leaderIndex = i;
					}
				}
			}
			this.list.remove(node);
			this.selfIndex = getSelfIndex();
		}
	}
	
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		sb.append("[");
		for(int i=0;i<this.list.size();i++)
		{
			if(i==this.selfIndex)
			{
				sb.append("(self)");
			}
			if(!this.electionInProgress && i==this.leaderIndex)
			{
				sb.append("(leader)");
			}
			sb.append(this.list.get(i).toString());
			if(i!=(this.list.size()-1))
			{
				sb.append(",");
			}
		}
		sb.append("]");
		return sb.toString();
	}
}
