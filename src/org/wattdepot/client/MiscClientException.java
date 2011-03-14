package org.wattdepot.client;

import org.restlet.data.Status;

/**
 * An exception that is thrown when the WattDepot client encounters an unusual error from the
 * server, such as an internal server error of some sort.
 * 
 * @author Robert Brewer
 */
public class MiscClientException extends WattDepotClientException {

  /**
   * The serialization UID, not that we'll ever use it.
   */
  private static final long serialVersionUID = 7381794970088811561L;

  /**
   * Thrown when the server indicates an unexpected error status.
   * 
   * @param status The Status instance indicating the problem.
   */
  public MiscClientException(Status status) {
    super(status);
  }

  /**
   * Thrown when the server indicates an unexpected error status.
   * 
   * @param status The Status instance indicating the problem.
   * @param error The previous error.
   */
  public MiscClientException(Status status, Throwable error) {
    super(status, error);
  }

  /**
   * Thrown when the server indicates an unexpected error status.
   * 
   * @param message String indicating the cause of the error.
   * @param error The previous error.
   */
  public MiscClientException(String message, Throwable error) {
    super(message, error);
  }
}
