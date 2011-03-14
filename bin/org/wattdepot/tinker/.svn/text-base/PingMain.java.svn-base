package org.wattdepot.tinker;

import org.restlet.Component;
import org.restlet.data.Protocol;

/**
 * Class used to start up the Ping server. Based on the code from the Restlet first steps
 * tutorial:
 * 
 * http://www.restlet.org/documentation/1.1/firstSteps
 * 
 * @author Robert Brewer
 */
public class PingMain {

  /**
   * Starts up the web server to respond to ping requests.
   * 
   * @param args String array of command line arguments
   */
  public static void main(String[] args) {
    try {
      // Create a new Component.
      Component component = new Component();

      // Add a new HTTP server listening on port 8181.
      component.getServers().add(Protocol.HTTP, 8181);

      // Attach the sample application.
      component.getDefaultHost().attach(new PingApplication());

      // Start the component.
      component.start();
    }
    catch (Exception e) {
      // Something is wrong.
      e.printStackTrace();
    }
//    PingClient.pingHost("http://127.0.0.1:8182/");
  }
}
