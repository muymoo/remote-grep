package org.uiuc.cs.distributed.grep;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

/**
 * Basic unit testing for a server node. Especially around setting a valid IP address.
 * 
 * @author evan
 */
public class NodeTest
{

    /**
     * construct a Node with a blank string
     * should fail
     */
    @Test
    public void testBlankString()
    {
        Node node = new Node("");

        boolean expected = false;
        boolean actual = node.isValid();
        String errorMessage = "A Node constructed with a blank string returned true for isValid";
        assertEquals(errorMessage, expected, actual);
    }

    /**
     * construct a Node with a valid IP but no port
     * should fail
     */
    @Test
    public void testValidIPNoPort()
    {
        Node node = new Node("130.126.112.146");

        boolean expected = false;
        boolean actual = node.isValid();
        String errorMessage = "A Node constructed with a valid IP but no port returned true for isValid";
        assertEquals(errorMessage, expected, actual);
    }
    
    /**
     * construct a Node with only port
     * should fail
     */
    @Test
    public void testValidIPOnlyPort()
    {
        Node node = new Node(":80");

        boolean expected = false;
        boolean actual = node.isValid();
        String errorMessage = "A Node constructed with no IP returned true for isValid";
        assertEquals(errorMessage, expected, actual);
    }
    
    /**
     * construct a Node with an invalid IP that is too short
     * should fail
     */
    @Test
    public void testInvalidIPTooShort()
    {
        Node node = new Node("130.126.112:80");

        boolean expected = false;
        boolean actual = node.isValid();
        String errorMessage = "A Node constructed with an invalid IP returned true for isValid";
        assertEquals(errorMessage, expected, actual);
    }
    
    /**
     * construct a Node with an invalid IP that is too short
     * should fail
     */
    @Test
    public void testInvalidIPTooShort2()
    {
        Node node = new Node("130.126.112.:80");

        boolean expected = false;
        boolean actual = node.isValid();
        String errorMessage = "A Node constructed with an invalid IP returned true for isValid";
        assertEquals(errorMessage, expected, actual);
    }
    
    /**
     * construct a Node with an invalid IP that has chars
     * should fail
     */
    @Test
    public void testInvalidIPHasChars()
    {
        Node node = new Node("130.126.112.ef:80");

        boolean expected = false;
        boolean actual = node.isValid();
        String errorMessage = "A Node constructed with an invalid IP returned true for isValid";
        assertEquals(errorMessage, expected, actual);
    }
    
    /**
     * construct a Node with an invalid IP that has only chars
     * should fail
     */
    @Test
    public void testInvalidIPAllChars()
    {
        Node node = new Node("efadfadsf:80");

        boolean expected = false;
        boolean actual = node.isValid();
        String errorMessage = "A Node constructed with an invalid IP returned true for isValid";
        assertEquals(errorMessage, expected, actual);
    }
    
    /**
     * construct a Node with a valid IP
     * should pass
     */
    @Test
    public void testValidIPAndPort()
    {
        Node node = new Node("130.126.112.25:80");

        boolean expected = true;
        boolean actual = node.isValid();
        String errorMessage = "A Node constructed with an invalid IP returned true for isValid";
        assertEquals(errorMessage, expected, actual);
    }
    
    // -------------------------------------------------
    
    /**
     * test that the node with the higher IP is returned correctly
     * should pass
     */
    @Test
    public void testCompareToTwoEqualNodes()
    {
        Node node = new Node("130.126.112.25:80");
        Node node2 = new Node("130.126.112.25:80");

        boolean expected = true;
        try {
	        boolean actual = node.compareTo(node2) == 0;
	        String errorMessage = "The two equal nodes were not returned as equal ";
	        assertEquals(errorMessage, expected, actual);
        } catch( Exception e) {
        	fail("An exception was thrown during the ipComparison of two valid nodes");
        }
    }
    
    /**
     * test that the node with the higher IP is returned correctly
     * should pass
     */
    @Test
    public void testCompareToFirstNodeHigherFirstOctet()
    {
        Node node = new Node("200.126.112.25:80");
        Node node2 = new Node("130.126.112.25:80");

        boolean expected = true;
        try {
	        boolean actual = node.compareTo(node2) > 0;
	        String errorMessage = "The first node was not returned as being greater than the second ";
	        assertEquals(errorMessage, expected, actual);
        } catch( Exception e) {
        	fail("An exception was thrown during the ipComparison of two valid nodes");
        }
    }
    
    /**
     * test that the node with the higher IP is returned correctly
     * should pass
     */
    @Test
    public void testCompareToSecondNodeHigherFirstOctet()
    {
        Node node = new Node("130.126.112.25:80");
        Node node2 = new Node("200.126.112.25:80");

        boolean expected = true;
        try {
	        boolean actual = node.compareTo(node2) < 0;
	        String errorMessage = "The second node was not returned as being greater than the first";
	        assertEquals(errorMessage, expected, actual);
        } catch( Exception e) {
        	fail("An exception was thrown during the ipComparison of two valid nodes");
        }
    }
    
    /**
     * test that the node with the higher IP is returned correctly
     * should pass
     */
    @Test
    public void testCompareToFirstNodeHigherSecondOctet()
    {
        Node node = new Node("130.127.112.25:80");
        Node node2 = new Node("130.126.112.25:80");

        boolean expected = true;
        try {
	        boolean actual = node.compareTo(node2) > 0;
	        String errorMessage = "The first node was not returned as being greater than the second ";
	        assertEquals(errorMessage, expected, actual);
        } catch( Exception e) {
        	fail("An exception was thrown during the ipComparison of two valid nodes");
        }
    }
    
    /**
     * test that the node with the higher IP is returned correctly
     * should pass
     */
    @Test
    public void testCompareToSecondNodeHigherSecondOctet()
    {
        Node node = new Node("130.126.112.25:80");
        Node node2 = new Node("130.127.112.25:80");

        boolean expected = true;
        try {
	        boolean actual = node.compareTo(node2) < 0;
	        String errorMessage = "The second node was not returned as being greater than the first";
	        assertEquals(errorMessage, expected, actual);
        } catch( Exception e) {
        	fail("An exception was thrown during the ipComparison of two valid nodes");
        }
    }
    
    /**
     * test that the node with the higher IP is returned correctly
     * should pass
     */
    @Test
    public void testCompareToFirstNodeHigherThirdOctet()
    {
        Node node = new Node("130.126.200.25:80");
        Node node2 = new Node("130.126.112.25:80");

        boolean expected = true;
        try {
	        boolean actual = node.compareTo(node2) > 0;
	        String errorMessage = "The first node was not returned as being greater than the second ";
	        assertEquals(errorMessage, expected, actual);
        } catch( Exception e) {
        	fail("An exception was thrown during the ipComparison of two valid nodes");
        }
    }
    
    /**
     * test that the node with the higher IP is returned correctly
     * should pass
     */
    @Test
    public void testCompareToSecondNodeHigherThirdOctet()
    {
        Node node = new Node("130.126.112.25:80");
        Node node2 = new Node("130.126.240.25:80");

        boolean expected = true;
        try {
	        boolean actual = node.compareTo(node2) < 0;
	        String errorMessage = "The second node was not returned as being greater than the first";
	        assertEquals(errorMessage, expected, actual);
        } catch( Exception e) {
        	fail("An exception was thrown during the ipComparison of two valid nodes");
        }
    }
    
    /**
     * test that the node with the higher IP is returned correctly
     * should pass
     */
    @Test
    public void testCompareToFirstNodeHigherFourthOctet()
    {
        Node node = new Node("130.126.112.90:80");
        Node node2 = new Node("130.126.112.25:80");

        boolean expected = true;
        try {
	        boolean actual = node.compareTo(node2) > 0;
	        String errorMessage = "The first node was not returned as being greater than the second ";
	        assertEquals(errorMessage, expected, actual);
        } catch( Exception e) {
        	fail("An exception was thrown during the ipComparison of two valid nodes");
        }
    }
    
    /**
     * test that the node with the higher IP is returned correctly
     * should pass
     */
    @Test
    public void testCompareToSecondNodeHigherFourthOctet()
    {
        Node node = new Node("130.126.112.25:80");
        Node node2 = new Node("130.126.112.255:80");

        boolean expected = true;
        try {
	        boolean actual = node.compareTo(node2) < 0;
	        String errorMessage = "The second node was not returned as being greater than the first";
	        assertEquals(errorMessage, expected, actual);
        } catch( Exception e) {
        	fail("An exception was thrown during the ipComparison of two valid nodes");
        }
    }
}
