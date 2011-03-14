package org.wattdepot.datainput;

import javax.xml.datatype.XMLGregorianCalendar;
import org.restlet.Client;
import org.restlet.data.ChallengeResponse;
import org.restlet.data.ChallengeScheme;
import org.restlet.data.MediaType;
import org.restlet.data.Method;
import org.restlet.data.Preference;
import org.restlet.data.Protocol;
import org.restlet.data.Reference;
import org.restlet.data.Request;
import org.restlet.data.Response;

/**
 * Provides a means to retrieve sensor data from a BMO server.
 *  
 * @author Robert Brewer
 */
public class BMONetworkClient {

  /** The URI to the BMO server. */
  protected String uri;
  
  /** The username for the BMO server. */
  protected String username;

  /** The password to the BMO server. */
  protected String password;

  /** The DB value for the BMO server. */
  protected String dbValue;

  /** The AS value for the BMO server. */
  protected String asValue;

  /**
   * Creates a new BMONetworkClient with the parameters needed to communicate with BMO.
   * 
   * @param uri The URI of the BMO server.
   * @param username The username for the BMO server. 
   * @param password The password for the BMO server.
   * @param dbValue The DB value for the BMO server.
   * @param asValue The AS value for the BMO server.
   */
  public BMONetworkClient(String uri, String username, String password, String dbValue,
      String asValue) {
    this.uri = uri;
    this.username = username;
    this.password = password;
    this.dbValue = dbValue;
    this.asValue = asValue;
  }
  
  /**
   * Makes an HTTP request to BMO server for meter data, returning the response from the server.
   * 
   * @param meterNumber The number of the meter data is to be fetched from.
   * @param startTime The desired starting point from which data will be retrieved.
   * @param endTime The desired starting point from which data will be retrieved.
   * @return The Response instance returned from the server.
   */
  public Response makeBMORequest(String meterNumber, XMLGregorianCalendar startTime,
      XMLGregorianCalendar endTime) {
    Client client = new Client(Protocol.HTTP);
    client.setConnectTimeout(2000);

    Reference reference = new Reference(this.uri);
    // Add query parameters needed by data download page
    reference.addQueryParameter("DB", this.dbValue);
    reference.addQueryParameter("AS", this.asValue);
    reference.addQueryParameter("MB", meterNumber);
    reference.addQueryParameter("DOWNLOAD", "YES");
    reference.addQueryParameter("DELIMITER", "TAB");
    reference.addQueryParameter("EXPORTTIMEZONE", "US%2FHawaii");
    reference.addQueryParameter("mnuStartYear", Integer.toString(startTime.getYear()));
    reference.addQueryParameter("mnuStartMonth", Integer.toString(startTime.getMonth()));
    reference.addQueryParameter("mnuStartDay", Integer.toString(startTime.getDay()));
    // mnuStartTime should look like 24-hour:minute, i.e. 0:31 or 10:13 or 23:46.
    // Need to zero prefix single digit minutes so we always have 2 digit minute strings
    int minutes = startTime.getMinute();
    String minuteString =
        (minutes < 10) ? "0" + Integer.toString(minutes) : Integer.toString(minutes);
    // addQueryParameter will do the URL encoding of ":" for us
    reference.addQueryParameter("mnuStartTime", Integer.toString(startTime.getHour()) + ":"
        + minuteString);
    reference.addQueryParameter("mnuEndYear", Integer.toString(endTime.getYear()));
    reference.addQueryParameter("mnuEndMonth", Integer.toString(endTime.getMonth()));
    reference.addQueryParameter("mnuEndDay", Integer.toString(endTime.getDay()));
    // mnuEndTime should look like 24-hour:minute, i.e. 0:31 or 10:13 or 23:46.
    // Need to zero prefix single digit minutes so we always have 2 digit minute strings
    minutes = endTime.getMinute();
    minuteString = (minutes < 10) ? "0" + Integer.toString(minutes) : Integer.toString(minutes);
    // addQueryParameter will do the URL encoding of ":" for us
    reference.addQueryParameter("mnuEndTime", Integer.toString(endTime.getHour()) + ":"
        + minuteString);
    Request request = new Request(Method.GET, reference);
    request.getClientInfo().getAcceptedMediaTypes().add(
        new Preference<MediaType>(MediaType.APPLICATION_ALL));
    ChallengeResponse authentication =
        new ChallengeResponse(ChallengeScheme.HTTP_BASIC, this.username, this.password);
    request.setChallengeResponse(authentication);
    return client.handle(request);
  }
}