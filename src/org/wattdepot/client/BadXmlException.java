package org.wattdepot.client;

import org.restlet.data.Status;

/**
 * An exception that is thrown when the WattDepot client receives XML from the server that it is
 * unable to unmarshall back into a Java object, or when the server reports that the XML it received
 * was bad.
 * 
 * @author Robert Brewer
 */
public class BadXmlException extends WattDepotClientException {

  /**
   * The serialization UID, not that we'll ever use it.
   */
  private static final long serialVersionUID = 1302799956613793285L;

  /**
   * Thrown when the server provides bad XML or server reports bad XML sent.
   * 
   * @param status The Status instance indicating the problem.
   */
  public BadXmlException(Status status) {
    super(status);
  }

  /**
   * Thrown when the server provides bad XML or server reports bad XML sent.
   * 
   * @param status The Status instance indicating the problem.
   * @param error The previous error.
   */
  public BadXmlException(Status status, Throwable error) {
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
