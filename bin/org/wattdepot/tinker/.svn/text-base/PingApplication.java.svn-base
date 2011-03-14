package org.wattdepot.tinker;

import org.restlet.Application;
import org.restlet.Restlet;
import org.restlet.Router;

/**
 * Web application that responds to ping requests.
 * 
 * @author Robert Brewer
 */
public class PingApplication extends Application {

  /**
   * Creates a root Restlet that will receive all incoming calls.
   * 
   * @return the newly created Restlet object
   */
  @Override
  public synchronized Restlet createRoot() {
    // Create a router Restlet that routes each call to a
    // new instance of PingResource.
    Router router = new Router(getContext());

    // Defines only one route
    router.attach("/ping", PingResource.class);
//    router.attachDefault(PingResource.class);

    return router;
  }
}
