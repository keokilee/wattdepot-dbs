package org.wattdepot.client;

import org.restlet.data.Status;

/**
 * An exception that is thrown when the WattDepot server indicates that the requested resource
 * cannot be found (an HTTP 404 status code).
 * 
 * @author Robert Brewer
 */
public class ResourceNotFoundException extends WattDepotClientException {

  /**
   * The serialization UID, not that we'll ever use it.
   */
  private static final long serialVersionUID = -1217598939639212243L;

  /**
   * Thrown when the server cannot find the requested resource.
   * 
   * @param status The Status instance indicating the problem.
   */
  public ResourceNotFoundException(Status status) {
    super(status);
  }

  /**
   * Thrown when the server cannot find the requested resource.
   * 
   * @param status The Status instance indicating the problem.
   * @param error The previous error.
   */
  public ResourceNotFoundException(Status status, Throwable error) {
    super(status, error);
  }
}
