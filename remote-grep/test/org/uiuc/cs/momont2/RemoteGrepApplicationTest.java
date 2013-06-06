package org.uiuc.cs.momont2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class RemoteGrepApplicationTest {

	private final static ByteArrayOutputStream outContent = new ByteArrayOutputStream();
	static RemoteGrepApplication remoteGrepApplication;

	@BeforeClass
	public static void initialize() {
		remoteGrepApplication = new RemoteGrepApplication();
		System.setOut(new PrintStream(outContent));
	}

	@AfterClass
	public static void cleanUp() {
		System.setOut(null);
	}

	@Test
	public void promptsUserForInput() {
		remoteGrepApplication.prompt();
		Assert.assertEquals(">>", outContent.toString());
	}

	@Test
	public void executesGrepCommand() {
		outContent.reset();

		ByteArrayInputStream in = new ByteArrayInputStream(
				"grep 'ERROR' machine.1.log".getBytes());
		System.setIn(in);
		remoteGrepApplication.readInput();
		Assert.assertEquals(
				"/tmp/machine.1.log:1:14:53 [ERROR] Cannot read machine code.",
				outContent.toString());
	}

}
