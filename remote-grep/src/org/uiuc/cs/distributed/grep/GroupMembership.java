package org.uiuc.cs.distributed.grep;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GroupMembership {
	
	public List<Node> list;
	
	public GroupMembership() {
		this.list = Collections
				.synchronizedList(new ArrayList<Node>());
	}
	
	public void add(Node node)
	{
		this.list.add(node);
	}
	
	public void remove(Node node)
	{
		this.list.remove(node);
	}
}
