package org.wattdepot.tinker;

import static org.junit.Assert.assertTrue;
import org.junit.BeforeClass;
import org.junit.Test;
import org.restlet.Component;
import org.restlet.data.Protocol;

/**
 * Tests the Ping API, which is very simple.
 * 
 * @author Robert Brewer
 */
public class TestPingApi {

  /**
   * Test that GET {host}/ping returns the hello world text.
   * 
   * @throws Exception If problems occur.
   */
  @Test
  public void testPing() throws Exception {
    assertTrue("Checking ping", PingClient.pingHost("http://127.0.0.1:8181/"));
  }
  
  /**
   * Starts the server going for these tests. 
   * @throws Exception If problems occur setting up the server. 
   */
  @BeforeClass public static void setupServer() throws Exception {
    // Create a new Component.
    Component component = new Component();

    // Add a new HTTP server listening on port 8181.
    component.getServers().add(Protocol.HTTP, 8181);

    // Attach the sample application.
    component.getDefaultHost().attach(new PingApplication());

    // Start the component.
    component.start();
  }
}
