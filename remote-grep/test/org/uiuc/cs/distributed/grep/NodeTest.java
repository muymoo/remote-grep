package org.uiuc.cs.distributed.grep;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class NodeTest {

	@Test
	public void testBlankIP() {
		Node node = new Node("",0);
		
		assertTrue
		fail("Not yet implemented");
	}
	
	public void testValidIPNoPort() {
		Node node = new Node();
		fail();
	}
	
	public void testValidIPValidPort() {
		fail();
	}
	
	public void testInvalidIPWithCharacters() {
		fail();
	}
	
	public void testInvalidIPWithTooFewPeriods() {
		fail();
	}
	
	public void testGetters() {
		fail();
	}

}
