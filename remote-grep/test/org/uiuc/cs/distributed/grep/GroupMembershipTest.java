package org.uiuc.cs.distributed.grep;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.Test;

public class GroupMembershipTest {
	
    /**
     * construct a list with the same 3 nodes
     * should fail
     */
    @Test
    public void testAllSameIPs()
    {
    	GroupMembership groupMembership = new GroupMembership("1.2.3.4:50");
    	groupMembership.add(new Node("1.2.3.4:50"));
    	groupMembership.add(new Node("1.2.3.4:50"));
    	groupMembership.add(new Node("1.2.3.4:50"));

        int expected = 1;
        int actual = groupMembership.list.size();
        String errorMessage = "A GroupMembership constructed with 3 of the same nodes were all added - fail";
        assertEquals(errorMessage, expected, actual);
    }
    
    /**
     * construct a list with the same 3 nodes out of order and make sure the 
     * list is sorted.
     */
    @Test
    public void testAddInOrderListByIP()
    {
    	GroupMembership groupMembership = new GroupMembership("1.2.3.4:50");
    	groupMembership.add(new Node("1.2.3.4:50"));
    	groupMembership.add(new Node("1.2.3.5:50"));
    	groupMembership.add(new Node("1.2.3.7:50"));

        boolean expected = true;
        boolean actual = groupMembership.list.size() == 3;
        String errorMessage = "A GroupMembership was not made with the correct number of nodes";
        assertEquals(errorMessage, expected, actual);
        
        expected = true;
        actual = groupMembership.list.get(0).getIP().compareTo("1.2.3.4") == 0;
        errorMessage = "Node(1.2.3.4) was not in the right position";
        assertEquals(errorMessage, expected, actual);
        expected = true;
        actual = groupMembership.list.get(1).getIP().compareTo("1.2.3.5") == 0;
        errorMessage = "Node(1.2.3.5) was not in the right position";
        assertEquals(errorMessage, expected, actual);
        expected = true;
        actual = groupMembership.list.get(2).getIP().compareTo("1.2.3.7") == 0;
        errorMessage = "Node(1.2.3.7) was not in the right position";
        assertEquals(errorMessage, expected, actual);
    }
    
    /**
     * construct a list with the same 3 nodes out of order and make sure the 
     * list is sorted.
     */
    @Test
    public void testAddResortsListByIP()
    {
    	GroupMembership groupMembership = new GroupMembership("1.2.3.4:50");
    	groupMembership.add(new Node("1.2.3.7:50"));
    	groupMembership.add(new Node("1.2.3.5:50"));
    	groupMembership.add(new Node("1.2.3.4:50"));

        boolean expected = true;
        boolean actual = groupMembership.list.size() == 3;
        String errorMessage = "A GroupMembership was not made with the correct number of nodes";
        assertEquals(errorMessage, expected, actual);
        
        expected = true;
        actual = groupMembership.list.get(0).getIP().compareTo("1.2.3.4") == 0;
        errorMessage = "Node(1.2.3.4) was not in the right position";
        assertEquals(errorMessage, expected, actual);
        expected = true;
        actual = groupMembership.list.get(1).getIP().compareTo("1.2.3.5") == 0;
        errorMessage = "Node(1.2.3.5) was not in the right position";
        assertEquals(errorMessage, expected, actual);
        expected = true;
        actual = groupMembership.list.get(2).getIP().compareTo("1.2.3.7") == 0;
        errorMessage = "Node(1.2.3.7) was not in the right position";
        assertEquals(errorMessage, expected, actual);
    }
    
    // ----------------------------------------------------------
    
    /**
     * create a group membership list and check that the self node is correctly assigned
     * should pass
     */
    @Test
    public void testCorrectSelfAssignment()
    {
    	GroupMembership groupMembership = new GroupMembership("1.2.3.5");
    	groupMembership.add(new Node("1.2.3.4:50"));
    	groupMembership.add(new Node("1.2.3.5:50"));
    	groupMembership.add(new Node("1.2.3.6:50"));

        boolean expected = true;
        boolean actual = groupMembership.getSelfIndex() == 1;
        String errorMessage = "A GroupMembership did not assign the correct node to itself.";
        assertEquals(errorMessage, expected, actual);	
    }
    
    /**
     * create a group membership list and check that the self node is correctly assigned
     * should pass
     */
    @Test
    public void testNoSelfAssignmentNotInList()
    {
    	GroupMembership groupMembership = new GroupMembership("1.2.3.7");
    	groupMembership.add(new Node("1.2.3.4:50"));
    	groupMembership.add(new Node("1.2.3.5:50"));
    	groupMembership.add(new Node("1.2.3.6:50"));

        boolean expected = true;
        boolean actual = groupMembership.getSelfIndex() < 0;
        String errorMessage = "A GroupMembership did not assign the correct node to itself.";
        assertEquals(errorMessage, expected, actual);	
    }
    
    /**
     * create an empty group membership list and check that the self node is correctly assigned
     * should pass
     */
    @Test
    public void testNoSelfAssignmentEmptyList()
    {
    	GroupMembership groupMembership = new GroupMembership("1.2.3.7");

        boolean expected = true;
        boolean actual = groupMembership.getSelfIndex() < 0;
        String errorMessage = "A GroupMembership did not assign the correct node to itself.";
        assertEquals(errorMessage, expected, actual);	
    }
    
    // ---------------------------------------------------------
    
    /**
     * create a group membership list and check that the send index nodes and receive index nodes are set correctly
     * should pass
     */
    @Test
    public void testCorrectSendReceiveIndexesSetWhenSelfIsFirstInTwo()
    {
    	GroupMembership groupMembership = new GroupMembership("1.2.3.4");
    	groupMembership.add(new Node("1.2.3.4:50"));
    	groupMembership.add(new Node("1.2.3.5:50"));
        Node receiveNode = groupMembership.getHeartbeatReceiveNode();
        
        boolean expected = true;
        boolean actual = receiveNode.getIP().equals("1.2.3.5");
        String errorMessage = "The incorrect node was set as the receive node";
        assertEquals(errorMessage, expected, actual);
        
        Node sendNode = groupMembership.getHeartbeatSendNode();
        
        expected = true;
        actual = sendNode.getIP().equals("1.2.3.5");
        errorMessage = "The incorrect node was set as the send node";
        assertEquals(errorMessage, expected, actual);
    }

    /**
     * create a group membership list and check that the send index nodes and receive index nodes are set correctly
     * should pass
     */
    @Test
    public void testCorrectSendReceiveIndexesSetWhenSelfIsSecondInTwo()
    {
    	GroupMembership groupMembership = new GroupMembership("1.2.3.6");
    	groupMembership.add(new Node("1.2.3.5:50"));
    	groupMembership.add(new Node("1.2.3.6:50"));
        Node receiveNode = groupMembership.getHeartbeatReceiveNode();
        
        boolean expected = true;
        boolean actual = receiveNode.getIP().equals("1.2.3.5");
        String errorMessage = "The incorrect node was set as the receive node";
        assertEquals(errorMessage, expected, actual);
        
        Node sendNode = groupMembership.getHeartbeatSendNode();
        
        expected = true;
        actual = sendNode.getIP().equals("1.2.3.5");
        errorMessage = "The incorrect node was set as the send node";
        assertEquals(errorMessage, expected, actual);
    }
    
    /**
     * create a group membership list and check that the send index nodes and receive index nodes are set correctly
     * should pass
     */
    @Test
    public void testCorrectSendReceiveIndexesSetWhenSelfIsMiddle()
    {
    	GroupMembership groupMembership = new GroupMembership("1.2.3.5");
    	groupMembership.add(new Node("1.2.3.4:50"));
    	groupMembership.add(new Node("1.2.3.5:50"));
    	groupMembership.add(new Node("1.2.3.6:50"));
        Node receiveNode = groupMembership.getHeartbeatReceiveNode();
        
        boolean expected = true;
        boolean actual = receiveNode.getIP().equals("1.2.3.6");
        String errorMessage = "The incorrect node was set as the receive node";
        assertEquals(errorMessage, expected, actual);
        
        Node sendNode = groupMembership.getHeartbeatSendNode();
        
        expected = true;
        actual = sendNode.getIP().equals("1.2.3.4");
        errorMessage = "The incorrect node was set as the send node";
        assertEquals(errorMessage, expected, actual);
    }
    
    /**
     * create a group membership list and check that the send index nodes and receive index nodes are set correctly
     * should pass
     */
    @Test
    public void testCorrectSendReceiveIndexesSetWhenSelfIsFirst()
    {
    	GroupMembership groupMembership = new GroupMembership("1.2.3.4");
    	groupMembership.add(new Node("1.2.3.4:50"));
    	groupMembership.add(new Node("1.2.3.5:50"));
    	groupMembership.add(new Node("1.2.3.6:50"));
        Node receiveNode = groupMembership.getHeartbeatReceiveNode();
        
        boolean expected = true;
        boolean actual = receiveNode.getIP().equals("1.2.3.5");
        String errorMessage = "The incorrect node was set as the receive node";
        assertEquals(errorMessage, expected, actual);
        
        Node sendNode = groupMembership.getHeartbeatSendNode();
        
        expected = true;
        actual = sendNode.getIP().equals("1.2.3.6");
        errorMessage = "The incorrect node was set as the send node";
        assertEquals(errorMessage, expected, actual);
    }

    /**
     * create a group membership list and check that the send index nodes and receive index nodes are set correctly
     * should pass
     */
    @Test
    public void testCorrectSendReceiveIndexesSetWhenSelfIsLast()
    {
    	GroupMembership groupMembership = new GroupMembership("1.2.3.6");
    	groupMembership.add(new Node("1.2.3.4:50"));
    	groupMembership.add(new Node("1.2.3.5:50"));
    	groupMembership.add(new Node("1.2.3.6:50"));
        Node receiveNode = groupMembership.getHeartbeatReceiveNode();
        
        boolean expected = true;
        boolean actual = receiveNode.getIP().equals("1.2.3.4");
        String errorMessage = "The incorrect node was set as the receive node";
        assertEquals(errorMessage, expected, actual);
        
        Node sendNode = groupMembership.getHeartbeatSendNode();
        
        expected = true;
        actual = sendNode.getIP().equals("1.2.3.5");
        errorMessage = "The incorrect node was set as the send node";
        assertEquals(errorMessage, expected, actual);
    }
    
    // ---------------------------------------------------------------
    
    /**
     * create a group membership list and check that the list of 
     * lower and upper members are created correctly 
     */
    @Test
    public void testGetMembersWhenSelfIsMiddle()
    {
    	GroupMembership groupMembership = new GroupMembership("1.2.3.5");
    	groupMembership.add(new Node("1.2.3.4:50"));
    	groupMembership.add(new Node("1.2.3.5:50"));
    	groupMembership.add(new Node("1.2.3.6:50"));
        List<Node> lowerMembers = groupMembership.getLowerMembers();
        

        String errorMessage = "The incorrect number of nodes were set in the lower members";
        assertEquals(errorMessage, lowerMembers.size(), 1);
        boolean expected = true;
        boolean actual = lowerMembers.get(0).getIP().compareTo("1.2.3.4") == 0;
        errorMessage = "The incorrect node was in the lower members";
        assertEquals(errorMessage, expected, actual);
        
        List<Node> upperMembers = groupMembership.getUpperMembers();
        errorMessage = "The incorrect number of nodes were set in the upper members";
        assertEquals(errorMessage, upperMembers.size(), 1);
        expected = true;
        actual = upperMembers.get(0).getIP().compareTo("1.2.3.6") == 0;
        errorMessage = "The incorrect node was in the upper members";
        assertEquals(errorMessage, expected, actual);
    }
    
    /**
     * create a group membership list and check that the list of 
     * lower and upper members are created correctly 
     */
    @Test
    public void testGetMembersWhenSelfIsFirst()
    {
    	GroupMembership groupMembership = new GroupMembership("1.2.3.4");
    	groupMembership.add(new Node("1.2.3.4:50"));
    	groupMembership.add(new Node("1.2.3.5:50"));
    	groupMembership.add(new Node("1.2.3.6:50"));
        List<Node> lowerMembers = groupMembership.getLowerMembers();
        

        String errorMessage = "The incorrect number of nodes were set in the lower members";
        assertEquals(errorMessage, lowerMembers.size(), 0);
        
        List<Node> upperMembers = groupMembership.getUpperMembers();
        errorMessage = "The incorrect number of nodes were set in the upper members";
        assertEquals(errorMessage, upperMembers.size(), 2);
        boolean expected = true;
        boolean actual = upperMembers.get(0).getIP().compareTo("1.2.3.5") == 0;
        errorMessage = "The incorrect node was in the upper members";
        assertEquals(errorMessage, expected, actual);
        expected = true;
        actual = upperMembers.get(1).getIP().compareTo("1.2.3.6") == 0;
        errorMessage = "The incorrect node was in the upper members";
        assertEquals(errorMessage, expected, actual);
    }
    
    /**
     * create a group membership list and check that the list of 
     * lower and upper members are created correctly 
     */
    @Test
    public void testGetMembersWhenSelfIsLast()
    {
    	GroupMembership groupMembership = new GroupMembership("1.2.3.6");
    	groupMembership.add(new Node("1.2.3.4:50"));
    	groupMembership.add(new Node("1.2.3.5:50"));
    	groupMembership.add(new Node("1.2.3.6:50"));
        List<Node> lowerMembers = groupMembership.getLowerMembers();
        

        String errorMessage = "The incorrect number of nodes were set in the lower members";
        assertEquals(errorMessage, lowerMembers.size(), 2);
        boolean expected = true;
        boolean actual = lowerMembers.get(0).getIP().compareTo("1.2.3.4") == 0;
        errorMessage = "The incorrect node was in the lower members";
        assertEquals(errorMessage, expected, actual);
        expected = true;
        actual = lowerMembers.get(1).getIP().compareTo("1.2.3.5") == 0;
        errorMessage = "The incorrect node was in the lower members";
        assertEquals(errorMessage, expected, actual);
        
        List<Node> upperMembers = groupMembership.getUpperMembers();
        errorMessage = "The incorrect number of nodes were set in the upper members";
        assertEquals(errorMessage, upperMembers.size(), 0);
    }

}
