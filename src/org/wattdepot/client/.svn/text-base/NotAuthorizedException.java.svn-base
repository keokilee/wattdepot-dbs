package org.wattdepot.client;

import org.restlet.data.Status;

/**
 * An exception that is thrown when the WattDepot client is not authorized to access the requested
 * resource (an HTTP 401 status code).
 * 
 * @author Robert Brewer
 */
public class NotAuthorizedException extends WattDepotClientException {

  /**
   * The serialization UID, not that we'll ever use it.
   */
  private static final long serialVersionUID = -7027662905501863042L;

  /**
   * Thrown when the server rejects a request as not authorized.
   * 
   * @param status The Status instance indicating the problem.
   */
  public NotAuthorizedException(Status status) {
    super(status);
  }

  /**
   * Thrown when the server rejects a request as not authorized.
   * 
   * @param status The Status instance indicating the problem.
   * @param error The previous error.
   */
  public NotAuthorizedException(Status status, Throwable error) {
    super(status, error);
  }

//  /** {@inheritDoc} */
//  public NotAuthorizedException(String description, Throwable error) {
//    super(description, error);
//  }
//
//  /** {@inheritDoc} */
//  public NotAuthorizedException(String description) {
//    super(description);
//  }
}
