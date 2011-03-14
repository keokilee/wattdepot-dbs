package org.wattdepot.tinker;

import java.io.IOException;
import org.restlet.Client;
import org.restlet.data.Method;
import org.restlet.data.Protocol;
import org.restlet.data.Request;
import org.restlet.data.Response;

/**
 * Simple client that provides access to the Ping API, such as it is.
 * 
 * @author Robert Brewer
 */
public class PingClient {

  /**
   * Pings the specified host to determine if PingServer is running on that host.
   * 
   * @param hostname the hostname to ping
   * @return true if PingServer server is running, false otherwise.
   */
  public static boolean pingHost(String hostname) {
    String registerUri = hostname.endsWith("/") ? hostname + "ping" : hostname + "/ping";
    Request request = new Request();
    request.setResourceRef(registerUri);
    request.setMethod(Method.GET);
    Client client = new Client(Protocol.HTTP);
    client.setConnectTimeout(2000);
    Response response = client.handle(request);
    if (response.getStatus().isSuccess()) {
      String pingText;
      try {
        pingText = response.getEntity().getText();
      }
      catch (IOException e) {
        return false;
      }
      return PingResource.HELLO_WORLD_TEXT.equals(pingText);
    }
    else {
      return false;
    }
  }
}
