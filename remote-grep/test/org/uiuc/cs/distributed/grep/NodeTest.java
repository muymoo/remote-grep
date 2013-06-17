package org.uiuc.cs.distributed.grep;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
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
}
