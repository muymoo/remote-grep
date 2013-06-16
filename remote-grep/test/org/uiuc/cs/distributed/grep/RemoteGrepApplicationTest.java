package org.uiuc.cs.distributed.grep;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.uiuc.cs.distributed.grep.RemoteGrepApplication;

public class RemoteGrepApplicationTest {

	private final static ByteArrayOutputStream outContent = new ByteArrayOutputStream();
	static RemoteGrepApplication remoteGrepApplication;

	@BeforeClass
	public static void initialize() {
		remoteGrepApplication = RemoteGrepApplication.getInstance();
		System.setOut(new PrintStream(outContent));
	}

	@AfterClass
	public static void cleanUp() {
		System.setOut(null);
	}

	@Test
	public void promptsUserForInput() {
		Assert.assertEquals(">>", outContent.toString());
	}

	@Test
	public void executesGrepCommand() {
		outContent.reset();

		ByteArrayInputStream in = new ByteArrayInputStream("1".getBytes());
				//"grep 'ERROR' machine.1.log".getBytes());
		System.setIn(in);
		Assert.assertEquals(
				"/tmp/machine.1.log:1:14:53 [ERROR] Cannot read machine code.",
				outContent.toString());
	}

//	@Test
//	public void listensOnSocket() {
//		remoteGrepApplication.startServer();
//		Assert.assertTrue(false);
//	}
}
