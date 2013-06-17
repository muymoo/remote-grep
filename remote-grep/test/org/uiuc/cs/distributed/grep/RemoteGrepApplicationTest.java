package org.uiuc.cs.distributed.grep;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import junit.framework.Assert;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.uiuc.cs.distributed.grep.util.TestLogs;


public class RemoteGrepApplicationTest
{

    private final static ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    static RemoteGrepApplication               remoteGrepApplication;

    @BeforeClass
    public static void initialize()
    {
        remoteGrepApplication = RemoteGrepApplication.getInstance();
        remoteGrepApplication.startGrepServer();
        
        System.setOut(new PrintStream(outContent));
        TestLogs.createLogFile(5,100);
    }

    @AfterClass
    public static void cleanUp()
    {
        System.setOut(null);
    }
    
    @Before
    public void setUp()
    {
        RemoteGrepApplication.addTaskForNode(new Node("localhost:4444"));
        // RemoteGrepApplication.addDefaultNodes();
    }

    @Test
    public void grepForDoubleLine()
    {
        RemoteGrepApplication.runGrepTasks("'There should be 2 of me.'");
        RemoteGrepApplication.joinGrepTasks();
        Assert.assertTrue(outContent.toString().contains("There should be 2 of me.\nThere should be 2 of me.\n"));
    }
    
    @Test
    public void grepWithFlags()
    {
        RemoteGrepApplication.runGrepTasks("-rni '2 of me'");
        RemoteGrepApplication.joinGrepTasks();
        Assert.assertTrue(outContent.toString().contains("/tmp/cs425_momontbowling2/machine.5.log:101:There should be 2 of me."));
    }
    
    @Test
    public void grepSevere()
    {
        RemoteGrepApplication.runGrepTasks("-rni severe");
        RemoteGrepApplication.joinGrepTasks();
        Assert.assertTrue(outContent.toString().contains("SEVERE"));
    }
    
    @Test
    public void grepWarning()
    {
        RemoteGrepApplication.runGrepTasks("-rni warning");
        RemoteGrepApplication.joinGrepTasks();
        Assert.assertTrue(outContent.toString().contains("WARNING"));
    }
    
    @Test
    public void grepRegex()
    {
        RemoteGrepApplication.runGrepTasks("'[1-9]\\+ of me'");
        RemoteGrepApplication.joinGrepTasks();
        Assert.assertTrue(outContent.toString().contains("There should be 2 of me.\nThere should be 2 of me.\n"));   
    }
}
