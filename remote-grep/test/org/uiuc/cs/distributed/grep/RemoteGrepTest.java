package org.uiuc.cs.distributed.grep;

import java.io.File;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.uiuc.cs.distributed.grep.Grep;

public class RemoteGrepTest {

	static Grep remoteGrep;

	@BeforeClass
	public static void initialize() {
		remoteGrep = new Grep();
	}

	@AfterClass
	public static void cleanUp() {
		remoteGrep.deleteDummyFiles();
	}

	@Test
	public void createsDummyLogFile() {
		File dummyFile = new File("/tmp/machine.1.log");
		Assert.assertTrue(dummyFile.isFile());
	}

	@Test
	public void grepFileForError() {
		String grepCommand = "grep 'ERROR' machine.1.log";
		String result = remoteGrep.grep(grepCommand);
		Assert.assertEquals(
				"/tmp/machine.1.log:1:14:53 [ERROR] Cannot read machine code.",
				result);
	}
}
