package org.wattdepot.client;

import org.restlet.data.Status;

/**
 * An exception that is thrown when the WattDepot server reports that the client has attempted to
 * store a resource with the same identifier as an existing resource. Per the API, resources cannot
 * be overwritten, they must be deleted and then re-stored.
 * 
 * @author Robert Brewer
 */
public class OverwriteAttemptedException extends WattDepotClientException {

  /**
   * The serialization UID, not that we'll ever use it.
   */
  private static final long serialVersionUID = -7294139275219946732L;

  /**
   * Thrown when the client has attempted to overwrite an existing resource.
   * 
   * @param status The Status instance indicating the problem.
   */
  public OverwriteAttemptedException(Status status) {
    super(status);
  }

  /**
   * Thrown when the client has attempted to overwrite an existing resource.
   * 
   * @param status The Status instance indicating the problem.
   * @param error The previous error.
   */
  public OverwriteAttemptedException(Status status, Throwable error) {
    super(status, error);
  }

  // public BadXmlException(String description, Throwable error) {
  // super(description, error);
  // }
  //
  // public BadXmlException(String description) {
  // super(description);
  // }
}
