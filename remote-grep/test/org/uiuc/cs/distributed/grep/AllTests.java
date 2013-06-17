package org.uiuc.cs.distributed.grep;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * Wrapper test suite to run all of our test classes. Usage:
 * 
 * <pre>
 * make runtests
 * </pre>
 * 
 * @author 204054399
 */
@RunWith(Suite.class)
@SuiteClasses(
{
        NodeTest.class, RemoteGrepApplicationTest.class
})
public class AllTests
{

}
