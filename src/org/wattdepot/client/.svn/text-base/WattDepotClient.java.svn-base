package org.wattdepot.client;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.datatype.XMLGregorianCalendar;
import org.restlet.Client;
import org.restlet.data.ChallengeResponse;
import org.restlet.data.ChallengeScheme;
import org.restlet.data.CharacterSet;
import org.restlet.data.Language;
import org.restlet.data.MediaType;
import org.restlet.data.Method;
import org.restlet.data.Preference;
import org.restlet.data.Protocol;
import org.restlet.data.Reference;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.data.Status;
import org.restlet.resource.Representation;
import org.restlet.resource.StringRepresentation;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.sensordata.jaxb.SensorDataIndex;
import org.wattdepot.resource.sensordata.jaxb.SensorDataRef;
import org.wattdepot.resource.sensordata.jaxb.SensorDatas;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.resource.source.jaxb.SourceIndex;
import org.wattdepot.resource.source.jaxb.SourceRef;
import org.wattdepot.resource.source.jaxb.Sources;
import org.wattdepot.resource.source.summary.jaxb.SourceSummary;
import org.wattdepot.resource.user.jaxb.User;
import org.wattdepot.resource.user.jaxb.UserIndex;
import org.wattdepot.resource.user.jaxb.UserRef;
import org.wattdepot.server.Server;
import org.wattdepot.util.UriUtils;
import org.wattdepot.util.logger.RestletLoggerUtil;

/**
 * Provides a high-level interface for Clients wishing to communicate with a WattDepot server.
 * 
 * @author Philip Johnson
 * @author Robert Brewer
 */
public class WattDepotClient {

  private static final String START_TIME_PARAM = "?startTime=";

  /** The representation type for XML. */
  private Preference<MediaType> XML_MEDIA = new Preference<MediaType>(MediaType.TEXT_XML);

  /** The representation type for plain text. */
  private Preference<MediaType> TEXT_MEDIA = new Preference<MediaType>(MediaType.TEXT_PLAIN);

  /** The HTTP authentication approach. */
  private ChallengeScheme scheme = ChallengeScheme.HTTP_BASIC;

  private String wattDepotUri;
  private String username;
  private String password;
  /** The Restlet Client instance used to communicate with the server. */
  private Client client;

  /** Users JAXBContext. */
  private static final JAXBContext userJAXB;
  /** SensorData JAXBContext. */
  private static final JAXBContext sensorDataJAXB;
  /** Source JAXBContext. */
  private static final JAXBContext sourceJAXB;
  /** SourceSummary JAXBContext. */
  private static final JAXBContext sourceSummaryJAXB;

  // JAXBContexts are thread safe, so we can share them across all instances and threads.
  // https://jaxb.dev.java.net/guide/Performance_and_thread_safety.html
  static {
    try {
      userJAXB = JAXBContext.newInstance(org.wattdepot.resource.user.jaxb.ObjectFactory.class);
      sensorDataJAXB =
          JAXBContext.newInstance(org.wattdepot.resource.sensordata.jaxb.ObjectFactory.class);
      sourceJAXB = JAXBContext.newInstance(org.wattdepot.resource.source.jaxb.ObjectFactory.class);
      sourceSummaryJAXB =
          JAXBContext.newInstance(org.wattdepot.resource.source.summary.jaxb.ObjectFactory.class);
    }
    catch (Exception e) {
      throw new RuntimeException("Couldn't create JAXB context instances.", e);
    }
  }

  /**
   * Initializes a new WattDepotClient.
   * 
   * @param hostUri The URI of the WattDepot server, such as 'http://localhost:9876/wattdepot/'.
   * Must end in "/"
   * @param username The username that we will use for authentication. Can be null if no
   * authentication is to be attempted.
   * @param password The password we will use for authentication. Can be null if no authentication
   * is to be attempted.
   */
  public WattDepotClient(String hostUri, String username, String password) {
    if ((hostUri == null) || ("".equals(hostUri))) {
      throw new IllegalArgumentException("hostname cannot be null or the empty string.");
    }
    // We allow null usernames and passwords, but if they are empty string, throw exception
    if ("".equals(username)) {
      throw new IllegalArgumentException("username cannot be the empty string.");
    }
    if ("".equals(password)) {
      throw new IllegalArgumentException("password cannot be the empty string.");
    }

    this.wattDepotUri = hostUri;
    this.username = username;
    this.password = password;
    // nuke the Restlet loggers
    RestletLoggerUtil.removeRestletLoggers();
    this.client = new Client(Protocol.HTTP);
    this.client.setConnectTimeout(2000);
  }

  /**
   * Initializes a new WattDepotClient using anonymous (public) access to the provided server URI.
   * 
   * @param hostUri The URI of the WattDepot server, such as 'http://localhost:9876/wattdepot/'.
   * Must end in "/"
   */
  public WattDepotClient(String hostUri) {
    this(hostUri, null, null);
  }

  /**
   * Determines whether this client has been configured for anonymous access or not. If no
   * credentials (or partially missing credentials) were provided to the constructor, then it is
   * assumed that the client will only attempt API calls at the Access Control Level of "None".
   * 
   * @return true if client is configured for anonymous access, false otherwise.
   */
  private boolean isAnonymous() {
    return (this.username == null) || (this.password == null);
  }

  /**
   * Does the housekeeping for making HTTP requests to WattDepot by a test or admin user, including
   * authentication if requested. It is only public to allow testing of the WattDepot server in
   * certain cases. It is not intended for client use.
   * 
   * @param method the HTTP method requested.
   * @param requestString A string, such as "users". Do not start the string with a '/' (it is
   * unneeded).
   * @param mediaPref Indication of what type of media the client prefers from the server. See
   * XML_MEDIA and TEXT_MEDIA constants.
   * @param entity The representation to be sent with the request, or null if not needed.
   * @return The Response instance returned from the server.
   */
  public Response makeRequest(Method method, String requestString, Preference<MediaType> mediaPref,
      Representation entity) {
    Reference reference = new Reference(this.wattDepotUri + requestString);
    Request request =
        (entity == null) ? new Request(method, reference) : new Request(method, reference, entity);
    request.getClientInfo().getAcceptedMediaTypes().add(mediaPref);
    if (!isAnonymous()) {
      ChallengeResponse authentication =
          new ChallengeResponse(scheme, this.username, this.password);
      request.setChallengeResponse(authentication);
    }
    return this.client.handle(request);
  }

  /**
   * Determines the health of a WattDepot server.
   * 
   * @return true if the server is healthy, false if not healthy
   */
  public boolean isHealthy() {
    // Make HEAD request, since we only care about status code
    Response response = makeRequest(Method.HEAD, Server.HEALTH_URI, TEXT_MEDIA, null);

    // If we get a success status code, then server is healthy
    return response.getStatus().isSuccess();
  }

  /**
   * Returns the health string from WattDepot server.
   * 
   * @return the health message from server, or null if unable to retrieve any message
   */
  public String getHealthString() {
    // Make the request
    Response response = makeRequest(Method.GET, Server.HEALTH_URI, TEXT_MEDIA, null);

    // Try to extract the response text and return it
    try {
      return response.getEntity().getText();
    }
    catch (IOException e) {
      return null;
    }
  }

  /**
   * Determines whether or not the credentials provided in the constructor are valid by attempting
   * to retrieve the User resource that corresponds to the username from the constructor.
   * 
   * @return true if the constructor credentials are valid, and false if they are invalid or object
   * was called with null credentials.
   */
  public boolean isAuthenticated() {
    // URI is host with user resource URL appended, followed by the configured username
    String usersUri = Server.USERS_URI + "/" + this.username;

    if (isAnonymous()) {
      return false;
    }
    else {
      Response response = makeRequest(Method.HEAD, usersUri, XML_MEDIA, null);
      return response.getStatus().isSuccess();
    }
  }

  /**
   * Requests a SensorDataIndex containing all the SensorData available for this source from the
   * server.
   * 
   * @param source The name of the Source.
   * @return The SensorDataIndex.
   * @throws NotAuthorizedException If the client is not authorized to retrieve the SensorData
   * index.
   * @throws ResourceNotFoundException If the source name provided doesn't exist on the server.
   * @throws BadXmlException If error is encountered unmarshalling the XML from the server.
   * @throws MiscClientException If error is encountered retrieving the resource, or some unexpected
   * problem is encountered.
   */
  public SensorDataIndex getSensorDataIndex(String source) throws NotAuthorizedException,
      ResourceNotFoundException, BadXmlException, MiscClientException {
    Response response =
        makeRequest(Method.GET, Server.SOURCES_URI + "/" + source + "/" + Server.SENSORDATA_URI,
            XML_MEDIA, null);
    Status status = response.getStatus();

    if (status.equals(Status.CLIENT_ERROR_UNAUTHORIZED)) {
      // credentials were unacceptable to server
      throw new NotAuthorizedException(status);
    }
    if (status.equals(Status.CLIENT_ERROR_NOT_FOUND)) {
      // an unknown source name was specified
      throw new ResourceNotFoundException(status);
    }
    if (status.isSuccess()) {
      try {
        String xmlString = response.getEntity().getText();
        Unmarshaller unmarshaller = sensorDataJAXB.createUnmarshaller();
        return (SensorDataIndex) unmarshaller.unmarshal(new StringReader(xmlString));
      }
      catch (IOException e) {
        // Error getting the text from the entity body, bad news
        throw new MiscClientException(status, e);
      }
      catch (JAXBException e) {
        // Got some XML we can't parse
        throw new BadXmlException(status, e);
      }
    }
    else {
      // Some totally unexpected non-success status code, just throw generic client exception
      throw new MiscClientException(status);
    }
  }

  /**
   * Requests a SensorDataIndex representing all the SensorData resources for the named Source such
   * that their timestamp is greater than or equal to the given start time and less than or equal to
   * the given end time from the server. When looking to retrieve all the sensor data objects from
   * the index, getSensorDatas is preferable to this method.
   * 
   * @param source The name of the Source.
   * @param startTime The start of the range.
   * @param endTime The end of the range.
   * @return The SensorDataIndex.
   * @throws NotAuthorizedException If the client is not authorized to retrieve the SensorData
   * index.
   * @throws ResourceNotFoundException If the source name provided doesn't exist on the server.
   * @throws BadXmlException If error is encountered unmarshalling the XML from the server.
   * @throws MiscClientException If error is encountered retrieving the resource, or some unexpected
   * problem is encountered.
   * @see org.wattdepot.client.WattDepotClient#getSensorDatas getSensorDatas
   */
  public SensorDataIndex getSensorDataIndex(String source, XMLGregorianCalendar startTime,
      XMLGregorianCalendar endTime) throws NotAuthorizedException, ResourceNotFoundException,
      BadXmlException, MiscClientException {
    String uri =
        Server.SOURCES_URI + "/" + source + "/" + Server.SENSORDATA_URI + "/" + START_TIME_PARAM
            + startTime.toXMLFormat() + "&" + "endTime=" + endTime.toXMLFormat();
    Response response = makeRequest(Method.GET, uri, XML_MEDIA, null);
    Status status = response.getStatus();

    if (status.equals(Status.CLIENT_ERROR_UNAUTHORIZED)) {
      // credentials were unacceptable to server
      throw new NotAuthorizedException(status);
    }
    if (status.equals(Status.CLIENT_ERROR_NOT_FOUND)) {
      // an unknown source name was specified
      throw new ResourceNotFoundException(status);
    }
    if (status.equals(Status.CLIENT_ERROR_BAD_REQUEST)) {
      // bad timestamp provided in URI
      throw new BadXmlException(status);
    }
    if (status.isSuccess()) {
      try {
        String xmlString = response.getEntity().getText();
        Unmarshaller unmarshaller = sensorDataJAXB.createUnmarshaller();
        return (SensorDataIndex) unmarshaller.unmarshal(new StringReader(xmlString));
      }
      catch (IOException e) {
        // Error getting the text from the entity body, bad news
        throw new MiscClientException(status, e);
      }
      catch (JAXBException e) {
        // Got some XML we can't parse
        throw new BadXmlException(status, e);
      }
    }
    else {
      // Some totally unexpected non-success status code, just throw generic client exception
      throw new MiscClientException(status);
    }
  }

  /**
   * Requests a List of SensorData representing all the SensorData resources for the named Source
   * such that their timestamp is greater than or equal to the given start time and less than or
   * equal to the given end time from the server. Fetches the data using the "fetchAll" REST API
   * parameter.
   * 
   * @param source The name of the Source.
   * @param startTime The start of the range.
   * @param endTime The end of the range.
   * @return The List of SensorData in the range.
   * @throws NotAuthorizedException If the client is not authorized to retrieve the SensorData
   * index.
   * @throws ResourceNotFoundException If the source name provided doesn't exist on the server.
   * @throws BadXmlException If error is encountered unmarshalling the XML from the server.
   * @throws MiscClientException If error is encountered retrieving the resource, or some unexpected
   * problem is encountered.
   */
  public List<SensorData> getSensorDatas(String source, XMLGregorianCalendar startTime,
      XMLGregorianCalendar endTime) throws NotAuthorizedException, ResourceNotFoundException,
      BadXmlException, MiscClientException {

    String uri =
        Server.SOURCES_URI + "/" + source + "/" + Server.SENSORDATA_URI + "/" + START_TIME_PARAM
            + startTime.toXMLFormat() + "&" + "endTime=" + endTime.toXMLFormat() + "&"
            + "fetchAll=true";
    Response response = makeRequest(Method.GET, uri, XML_MEDIA, null);
    Status status = response.getStatus();

    if (status.equals(Status.CLIENT_ERROR_UNAUTHORIZED)) {
      // credentials were unacceptable to server
      throw new NotAuthorizedException(status);
    }
    if (status.equals(Status.CLIENT_ERROR_NOT_FOUND)) {
      // an unknown source name was specified
      throw new ResourceNotFoundException(status);
    }
    if (status.equals(Status.CLIENT_ERROR_BAD_REQUEST)) {
      // bad timestamp provided in URI
      throw new BadXmlException(status);
    }
    if (status.isSuccess()) {
      try {
        String xmlString = response.getEntity().getText();
        Unmarshaller unmarshaller = sensorDataJAXB.createUnmarshaller();
        return ((SensorDatas) unmarshaller.unmarshal(new StringReader(xmlString))).getSensorData();
      }
      catch (IOException e) {
        // Error getting the text from the entity body, bad news
        throw new MiscClientException(status, e);
      }
      catch (JAXBException e) {
        // Got some XML we can't parse
        throw new BadXmlException(status, e);
      }
    }
    else {
      // Some totally unexpected non-success status code, just throw generic client exception
      throw new MiscClientException(status);
    }

  }

  /**
   * Requests the SensorData from a given Source corresponding to the given timestamp.
   * 
   * @param source The name of the Source.
   * @param timestamp The timestamp of the desired SensorData.
   * @return The SensorData.
   * @throws NotAuthorizedException If the client is not authorized to retrieve the SensorData
   * index.
   * @throws ResourceNotFoundException If the source name provided doesn't exist on the server.
   * @throws BadXmlException If error is encountered unmarshalling the XML from the server.
   * @throws MiscClientException If error is encountered retrieving the resource, or some unexpected
   * problem is encountered.
   */
  public SensorData getSensorData(String source, XMLGregorianCalendar timestamp)
      throws NotAuthorizedException, ResourceNotFoundException, BadXmlException,
      MiscClientException {
    Response response =
        makeRequest(Method.GET, Server.SOURCES_URI + "/" + source + "/" + Server.SENSORDATA_URI
            + "/" + timestamp.toXMLFormat(), XML_MEDIA, null);
    Status status = response.getStatus();

    if (status.equals(Status.CLIENT_ERROR_UNAUTHORIZED)) {
      // credentials were unacceptable to server
      throw new NotAuthorizedException(status);
    }
    if (status.equals(Status.CLIENT_ERROR_BAD_REQUEST)) {
      // bad timestamp provided in URI
      throw new BadXmlException(status);
    }
    if (status.equals(Status.CLIENT_ERROR_NOT_FOUND)) {
      // an unknown source name was specified, or timestamp could not be found
      throw new ResourceNotFoundException(status);
    }
    if (status.isSuccess()) {
      try {
        String xmlString = response.getEntity().getText();
        Unmarshaller unmarshaller = sensorDataJAXB.createUnmarshaller();
        return (SensorData) unmarshaller.unmarshal(new StringReader(xmlString));
      }
      catch (IOException e) {
        // Error getting the text from the entity body, bad news
        throw new MiscClientException(status, e);
      }
      catch (JAXBException e) {
        // Got some XML we can't parse
        throw new BadXmlException(status, e);
      }
    }
    else {
      // Some totally unexpected non-success status code, just throw generic client exception
      throw new MiscClientException(status);
    }
  }

  /**
   * Requests the latest SensorData from a given Source.
   * 
   * @param source The name of the Source.
   * @return The SensorData.
   * @throws NotAuthorizedException If the client is not authorized to retrieve the SensorData
   * index.
   * @throws ResourceNotFoundException If the source name provided doesn't exist on the server, or
   * the source has no sensor data.
   * @throws BadXmlException If error is encountered unmarshalling the XML from the server.
   * @throws MiscClientException If error is encountered retrieving the resource, or some unexpected
   * problem is encountered.
   */
  public SensorData getLatestSensorData(String source) throws NotAuthorizedException,
      ResourceNotFoundException, BadXmlException, MiscClientException {
    Response response =
        makeRequest(Method.GET, Server.SOURCES_URI + "/" + source + "/" + Server.SENSORDATA_URI
            + "/" + Server.LATEST, XML_MEDIA, null);
    Status status = response.getStatus();

    if (status.equals(Status.CLIENT_ERROR_UNAUTHORIZED)) {
      // credentials were unacceptable to server
      throw new NotAuthorizedException(status);
    }
    if (status.equals(Status.CLIENT_ERROR_BAD_REQUEST)) {
      // bad timestamp provided in URI
      throw new BadXmlException(status);
    }
    if (status.equals(Status.CLIENT_ERROR_NOT_FOUND)) {
      // an unknown source name was specified, or timestamp could not be found
      throw new ResourceNotFoundException(status);
    }
    if (status.isSuccess()) {
      try {
        String xmlString = response.getEntity().getText();
        Unmarshaller unmarshaller = sensorDataJAXB.createUnmarshaller();
        return (SensorData) unmarshaller.unmarshal(new StringReader(xmlString));
      }
      catch (IOException e) {
        // Error getting the text from the entity body, bad news
        throw new MiscClientException(status, e);
      }
      catch (JAXBException e) {
        // Got some XML we can't parse
        throw new BadXmlException(status, e);
      }
    }
    else {
      // Some totally unexpected non-success status code, just throw generic client exception
      throw new MiscClientException(status);
    }
  }

  /**
   * Requests the latest SensorData from a given Source, and extracts the provided property key,
   * converts it to double and returns the value.
   * 
   * @param source The name of the Source.
   * @param key The property key to search for.
   * @return A double representing the the property at the given timestamp, or 0 if there is no
   * data.
   * @throws NotAuthorizedException If the client is not authorized to retrieve the SensorData
   * index.
   * @throws ResourceNotFoundException If the source name provided doesn't exist on the server, or
   * the source has no sensor data.
   * @throws BadXmlException If error is encountered unmarshalling the XML from the server.
   * @throws MiscClientException If error is encountered retrieving the resource, or some unexpected
   * problem is encountered.
   */
  private double getLatestSensorDataValue(String source, String key) throws NotAuthorizedException,
      ResourceNotFoundException, BadXmlException, MiscClientException {
    SensorData data = getLatestSensorData(source);
    return data.getProperties().getPropertyAsDouble(key);
  }

  /**
   * Requests the latest power generated from a given Source. The resulting value is in watts.
   * 
   * @param source The name of the Source.
   * @return A double representing the latest power generated, or 0 if there is no data.
   * @throws NotAuthorizedException If the client is not authorized to retrieve the power.
   * @throws ResourceNotFoundException If the source name provided doesn't exist on the server, or
   * the source has no sensor data.
   * @throws BadXmlException If error is encountered unmarshalling the XML from the server.
   * @throws MiscClientException If error is encountered retrieving the resource, or some unexpected
   * problem is encountered.
   */
  public double getLatestPowerGenerated(String source) throws NotAuthorizedException,
      ResourceNotFoundException, BadXmlException, MiscClientException {
    return getLatestSensorDataValue(source, SensorData.POWER_GENERATED);
  }

  /**
   * Requests the latest power consumed by a given Source. The resulting value is in watts.
   * 
   * @param source The name of the Source.
   * @return A double representing the latest power consumed, or 0 if there is no data.
   * @throws NotAuthorizedException If the client is not authorized to retrieve the power.
   * @throws ResourceNotFoundException If the source name provided doesn't exist on the server, or
   * the source has no sensor data.
   * @throws BadXmlException If error is encountered unmarshalling the XML from the server.
   * @throws MiscClientException If error is encountered retrieving the resource, or some unexpected
   * problem is encountered.
   */
  public double getLatestPowerConsumed(String source) throws NotAuthorizedException,
      ResourceNotFoundException, BadXmlException, MiscClientException {
    return getLatestSensorDataValue(source, SensorData.POWER_CONSUMED);
  }

  /**
   * Requests the latest energy generated to date from a given Source. The resulting value is in
   * watt hours.
   * 
   * @param source The name of the Source.
   * @return A double representing the latest energy generated to date, or 0 if there is no data.
   * @throws NotAuthorizedException If the client is not authorized to retrieve the power.
   * @throws ResourceNotFoundException If the source name provided doesn't exist on the server, or
   * the source has no sensor data.
   * @throws BadXmlException If error is encountered unmarshalling the XML from the server.
   * @throws MiscClientException If error is encountered retrieving the resource, or some unexpected
   * problem is encountered.
   */
  public double getLatestEnergyGeneratedToDate(String source) throws NotAuthorizedException,
      ResourceNotFoundException, BadXmlException, MiscClientException {
    return getLatestSensorDataValue(source, SensorData.ENERGY_GENERATED_TO_DATE);
  }

  /**
   * Requests the latest energy consumed to date from a given Source. The resulting value is in watt
   * hours.
   * 
   * @param source The name of the Source.
   * @return A double representing the latest energy consumed to date, or 0 if there is no data.
   * @throws NotAuthorizedException If the client is not authorized to retrieve the power.
   * @throws ResourceNotFoundException If the source name provided doesn't exist on the server, or
   * the source has no sensor data.
   * @throws BadXmlException If error is encountered unmarshalling the XML from the server.
   * @throws MiscClientException If error is encountered retrieving the resource, or some unexpected
   * problem is encountered.
   */
  public double getLatestEnergyConsumedToDate(String source) throws NotAuthorizedException,
      ResourceNotFoundException, BadXmlException, MiscClientException {
    return getLatestSensorDataValue(source, SensorData.ENERGY_CONSUMED_TO_DATE);
  }

  /**
   * Requests the power in SensorData format from a given Source corresponding to the given
   * timestamp. If you are just looking to retrieve the power values as a double, the
   * getPowerGenerated and getPowerConsumed methods are preferable to this one.
   * 
   * @param source The name of the Source.
   * @param timestamp The timestamp of the desired power reading.
   * @return The SensorData.
   * @throws NotAuthorizedException If the client is not authorized to retrieve the SensorData.
   * @throws ResourceNotFoundException If the source name provided doesn't exist on the server.
   * @throws BadXmlException If error is encountered unmarshalling the XML from the server.
   * @throws MiscClientException If error is encountered retrieving the resource, or some unexpected
   * problem is encountered.
   * @see org.wattdepot.client.WattDepotClient#getPowerGenerated getPowerGenerated
   * @see org.wattdepot.client.WattDepotClient#getPowerConsumed getPowerConsumed
   */
  public SensorData getPower(String source, XMLGregorianCalendar timestamp)
      throws NotAuthorizedException, ResourceNotFoundException, BadXmlException,
      MiscClientException {
    Response response =
        makeRequest(Method.GET, Server.SOURCES_URI + "/" + source + "/" + Server.POWER_URI + "/"
            + timestamp.toXMLFormat(), XML_MEDIA, null);
    Status status = response.getStatus();

    if (status.equals(Status.CLIENT_ERROR_UNAUTHORIZED)) {
      // credentials were unacceptable to server
      throw new NotAuthorizedException(status);
    }
    if (status.equals(Status.CLIENT_ERROR_BAD_REQUEST)) {
      // bad timestamp provided in URI
      throw new BadXmlException(status);
    }
    if (status.equals(Status.CLIENT_ERROR_NOT_FOUND)) {
      // an unknown source name was specified
      throw new ResourceNotFoundException(status);
    }
    if (status.isSuccess()) {
      try {
        String xmlString = response.getEntity().getText();
        Unmarshaller unmarshaller = sensorDataJAXB.createUnmarshaller();
        return (SensorData) unmarshaller.unmarshal(new StringReader(xmlString));
      }
      catch (IOException e) {
        // Error getting the text from the entity body, bad news
        throw new MiscClientException(status, e);
      }
      catch (JAXBException e) {
        // Got some XML we can't parse
        throw new BadXmlException(status, e);
      }
    }
    else {
      // Some totally unexpected non-success status code, just throw generic client exception
      throw new MiscClientException(status);
    }
  }

  /**
   * Requests the power from a given Source corresponding to the given timestamp, and extracts the
   * provided property key, converts it to double and returns the value.
   * 
   * @param source The name of the Source.
   * @param timestamp The timestamp of the desired power reading.
   * @param key The property key to search for.
   * @return A double representing the the property at the given timestamp, or 0 if there is no
   * data.
   * @throws NotAuthorizedException If the client is not authorized to retrieve the power.
   * @throws ResourceNotFoundException If the source name provided doesn't exist on the server.
   * @throws BadXmlException If error is encountered unmarshalling the XML from the server.
   * @throws MiscClientException If error is encountered retrieving the resource, or some unexpected
   * problem is encountered.
   */
  private double getPowerValue(String source, XMLGregorianCalendar timestamp, String key)
      throws NotAuthorizedException, ResourceNotFoundException, BadXmlException,
      MiscClientException {
    SensorData data = getPower(source, timestamp);
    return data.getProperties().getPropertyAsDouble(key);
  }

  /**
   * Requests the power generated from a given Source corresponding to the given timestamp. The
   * resulting value is in watts.
   * 
   * @param source The name of the Source.
   * @param timestamp The timestamp of the desired power reading.
   * @return A double representing the power generated at the given timestamp, or 0 if there is no
   * data.
   * @throws NotAuthorizedException If the client is not authorized to retrieve the power.
   * @throws ResourceNotFoundException If the source name provided doesn't exist on the server.
   * @throws BadXmlException If error is encountered unmarshalling the XML from the server.
   * @throws MiscClientException If error is encountered retrieving the resource, or some unexpected
   * problem is encountered.
   */
  public double getPowerGenerated(String source, XMLGregorianCalendar timestamp)
      throws NotAuthorizedException, ResourceNotFoundException, BadXmlException,
      MiscClientException {
    return getPowerValue(source, timestamp, SensorData.POWER_GENERATED);
  }

  /**
   * Requests the power consumed by a given Source corresponding to the given timestamp. The
   * resulting value is in watts.
   * 
   * @param source The name of the Source.
   * @param timestamp The timestamp of the desired power reading.
   * @return A double representing the power consumed at the given timestamp, or 0 if there is no
   * data.
   * @throws NotAuthorizedException If the client is not authorized to retrieve the power.
   * @throws ResourceNotFoundException If the source name provided doesn't exist on the server.
   * @throws BadXmlException If error is encountered unmarshalling the XML from the server.
   * @throws MiscClientException If error is encountered retrieving the resource, or some unexpected
   * problem is encountered.
   */
  public double getPowerConsumed(String source, XMLGregorianCalendar timestamp)
      throws NotAuthorizedException, ResourceNotFoundException, BadXmlException,
      MiscClientException {
    return getPowerValue(source, timestamp, SensorData.POWER_CONSUMED);
  }

  /**
   * Requests the energy in SensorData format from a given Source corresponding to the given
   * startTime and endTime and sampling interval in minutes. If you are just looking to retrieve the
   * energy values as doubles, the getEnergyGenerated and getEnergyConsumed methods are preferable
   * to this one.
   * 
   * @param source The name of the Source.
   * @param startTime The timestamp of the start of the range.
   * @param endTime The timestamp of the end of the range.
   * @param samplingInterval The sampling interval in minutes. A value of 0 tells the server to use
   * a default interval.
   * @return The SensorData.
   * @throws NotAuthorizedException If the client is not authorized to retrieve the SensorData.
   * @throws ResourceNotFoundException If the source name provided doesn't exist on the server.
   * @throws BadXmlException If error is encountered unmarshalling the XML from the server.
   * @throws MiscClientException If error is encountered retrieving the resource, or some unexpected
   * problem is encountered.
   * @see org.wattdepot.client.WattDepotClient#getEnergyGenerated getEnergyGenerated
   * @see org.wattdepot.client.WattDepotClient#getEnergyConsumed getEnergyConsumed
   */
  public SensorData getEnergy(String source, XMLGregorianCalendar startTime,
      XMLGregorianCalendar endTime, int samplingInterval) throws NotAuthorizedException,
      ResourceNotFoundException, BadXmlException, MiscClientException {
    String uriString =
        Server.SOURCES_URI + "/" + source + "/" + Server.ENERGY_URI + "/" + START_TIME_PARAM
            + startTime.toXMLFormat() + "&endTime=" + endTime.toXMLFormat();
    if (samplingInterval > 0) {
      // client provided sampling interval, so pass to server
      uriString = uriString + "&samplingInterval=" + Integer.toString(samplingInterval);
    }
    Response response = makeRequest(Method.GET, uriString, XML_MEDIA, null);
    Status status = response.getStatus();

    if (status.equals(Status.CLIENT_ERROR_UNAUTHORIZED)) {
      // credentials were unacceptable to server
      throw new NotAuthorizedException(status);
    }
    if (status.equals(Status.CLIENT_ERROR_BAD_REQUEST)) {
      // bad timestamp provided in URI
      throw new BadXmlException(status);
    }
    if (status.equals(Status.CLIENT_ERROR_NOT_FOUND)) {
      // an unknown source name was specified
      throw new ResourceNotFoundException(status);
    }
    if (status.isSuccess()) {
      try {
        String xmlString = response.getEntity().getText();
        Unmarshaller unmarshaller = sensorDataJAXB.createUnmarshaller();
        return (SensorData) unmarshaller.unmarshal(new StringReader(xmlString));
      }
      catch (IOException e) {
        // Error getting the text from the entity body, bad news
        throw new MiscClientException(status, e);
      }
      catch (JAXBException e) {
        // Got some XML we can't parse
        throw new BadXmlException(status, e);
      }
    }
    else {
      // Some totally unexpected non-success status code, just throw generic client exception
      throw new MiscClientException(status);
    }
  }

  /**
   * Requests the energy from a given Source corresponding to the range from startTime to endTime
   * and sampling interval in minutes, and extracts the provided property key, converts it to double
   * and returns the value.
   * 
   * @param source The name of the Source.
   * @param startTime The timestamp of the start of the range.
   * @param endTime The timestamp of the end of the range.
   * @param samplingInterval The sampling interval in minutes.
   * @param key The property key to search for.
   * @return A double representing the property at the given timestamp, or 0 if there is no data.
   * @throws NotAuthorizedException If the client is not authorized to retrieve the power.
   * @throws ResourceNotFoundException If the source name provided doesn't exist on the server.
   * @throws BadXmlException If error is encountered unmarshalling the XML from the server.
   * @throws MiscClientException If error is encountered retrieving the resource, or some unexpected
   * problem is encountered.
   */
  private double getEnergyValue(String source, XMLGregorianCalendar startTime,
      XMLGregorianCalendar endTime, int samplingInterval, String key)
      throws NotAuthorizedException, ResourceNotFoundException, BadXmlException,
      MiscClientException {
    SensorData data = getEnergy(source, startTime, endTime, samplingInterval);
    return data.getProperties().getPropertyAsDouble(key);
  }

  /**
   * Requests the energy generated from a given Source corresponding to the range from startTime to
   * endTime and sampling interval in minutes, and returns the value as a double in units of
   * watt-hours.
   * 
   * @param source The name of the Source.
   * @param startTime The timestamp of the start of the range.
   * @param endTime The timestamp of the end of the range.
   * @param samplingInterval The sampling interval in minutes.
   * @return A double representing the energy generated over the given range, or 0 if there is no
   * data.
   * @throws NotAuthorizedException If the client is not authorized to retrieve the power.
   * @throws ResourceNotFoundException If the source name provided doesn't exist on the server.
   * @throws BadXmlException If error is encountered unmarshalling the XML from the server.
   * @throws MiscClientException If error is encountered retrieving the resource, or some unexpected
   * problem is encountered.
   */
  public double getEnergyGenerated(String source, XMLGregorianCalendar startTime,
      XMLGregorianCalendar endTime, int samplingInterval) throws NotAuthorizedException,
      ResourceNotFoundException, BadXmlException, MiscClientException {
    return getEnergyValue(source, startTime, endTime, samplingInterval, SensorData.ENERGY_GENERATED);
  }

  /**
   * Requests the energy consumed by a given Source corresponding to the range from startTime to
   * endTime and sampling interval in minutes, and returns the value as a double in units of
   * watt-hours.
   * 
   * @param source The name of the Source.
   * @param startTime The timestamp of the start of the range.
   * @param endTime The timestamp of the end of the range.
   * @param samplingInterval The sampling interval in minutes.
   * @return A double representing the energy consumed over the given range, or 0 if there is no
   * data.
   * @throws NotAuthorizedException If the client is not authorized to retrieve the power.
   * @throws ResourceNotFoundException If the source name provided doesn't exist on the server.
   * @throws BadXmlException If error is encountered unmarshalling the XML from the server.
   * @throws MiscClientException If error is encountered retrieving the resource, or some unexpected
   * problem is encountered.
   */
  public double getEnergyConsumed(String source, XMLGregorianCalendar startTime,
      XMLGregorianCalendar endTime, int samplingInterval) throws NotAuthorizedException,
      ResourceNotFoundException, BadXmlException, MiscClientException {
    return getEnergyValue(source, startTime, endTime, samplingInterval, SensorData.ENERGY_CONSUMED);
  }

  /**
   * Requests the carbon emitted in SensorData format from a given Source corresponding to the given
   * startTime and endTime and sampling interval in minutes. If you are just looking to retrieve the
   * carbon emitted as a double, the getCarbonEmitted method is preferable to this one.
   * 
   * @param source The name of the Source.
   * @param startTime The timestamp of the start of the range.
   * @param endTime The timestamp of the end of the range.
   * @param samplingInterval The sampling interval in minutes. A value of 0 tells the server to use
   * a default interval.
   * @return The SensorData.
   * @throws NotAuthorizedException If the client is not authorized to retrieve the SensorData.
   * @throws ResourceNotFoundException If the source name provided doesn't exist on the server.
   * @throws BadXmlException If error is encountered unmarshalling the XML from the server.
   * @throws MiscClientException If error is encountered retrieving the resource, or some unexpected
   * problem is encountered.
   * @see org.wattdepot.client.WattDepotClient#getCarbonEmitted getCarbonEmitted
   */
  public SensorData getCarbon(String source, XMLGregorianCalendar startTime,
      XMLGregorianCalendar endTime, int samplingInterval) throws NotAuthorizedException,
      ResourceNotFoundException, BadXmlException, MiscClientException {
    String uriString =
        Server.SOURCES_URI + "/" + source + "/" + Server.CARBON_URI + "/" + START_TIME_PARAM
            + startTime.toXMLFormat() + "&endTime=" + endTime.toXMLFormat();
    if (samplingInterval > 0) {
      // client provided sampling interval, so pass to server
      uriString = uriString + "&samplingInterval=" + Integer.toString(samplingInterval);
    }
    Response response = makeRequest(Method.GET, uriString, XML_MEDIA, null);
    Status status = response.getStatus();

    if (status.equals(Status.CLIENT_ERROR_UNAUTHORIZED)) {
      // credentials were unacceptable to server
      throw new NotAuthorizedException(status);
    }
    if (status.equals(Status.CLIENT_ERROR_BAD_REQUEST)) {
      // bad timestamp provided in URI
      throw new BadXmlException(status);
    }
    if (status.equals(Status.CLIENT_ERROR_NOT_FOUND)) {
      // an unknown source name was specified
      throw new ResourceNotFoundException(status);
    }
    if (status.isSuccess()) {
      try {
        String xmlString = response.getEntity().getText();
        Unmarshaller unmarshaller = sensorDataJAXB.createUnmarshaller();
        return (SensorData) unmarshaller.unmarshal(new StringReader(xmlString));
      }
      catch (IOException e) {
        // Error getting the text from the entity body, bad news
        throw new MiscClientException(status, e);
      }
      catch (JAXBException e) {
        // Got some XML we can't parse
        throw new BadXmlException(status, e);
      }
    }
    else {
      // Some totally unexpected non-success status code, just throw generic client exception
      throw new MiscClientException(status);
    }
  }

  /**
   * Requests the carbon emitted from a given Source corresponding to the range from startTime to
   * endTime and sampling interval in minutes, and returns the value as a double in units of lbs CO2
   * equivalent.
   * 
   * @param source The name of the Source.
   * @param startTime The timestamp of the start of the range.
   * @param endTime The timestamp of the end of the range.
   * @param samplingInterval The sampling interval in minutes.
   * @return A double representing the carbon emitted over the given range, or 0 if there is no
   * data.
   * @throws NotAuthorizedException If the client is not authorized to retrieve the power.
   * @throws ResourceNotFoundException If the source name provided doesn't exist on the server.
   * @throws BadXmlException If error is encountered unmarshalling the XML from the server.
   * @throws MiscClientException If error is encountered retrieving the resource, or some unexpected
   * problem is encountered.
   */
  public double getCarbonEmitted(String source, XMLGregorianCalendar startTime,
      XMLGregorianCalendar endTime, int samplingInterval) throws NotAuthorizedException,
      ResourceNotFoundException, BadXmlException, MiscClientException {
    SensorData data = getCarbon(source, startTime, endTime, samplingInterval);
    return data.getProperties().getPropertyAsDouble(SensorData.CARBON_EMITTED);
  }

  /**
   * Convenience method for retrieving a SensorData given its SensorDataRef.
   * 
   * @param ref The SensorDataRef of the desired SensorData.
   * @return The SensorData.
   * @throws NotAuthorizedException If the client is not authorized to retrieve the SensorData
   * index.
   * @throws ResourceNotFoundException If the source name in the ref doesn't exist on the server.
   * @throws BadXmlException If error is encountered unmarshalling the XML from the server.
   * @throws MiscClientException If error is encountered retrieving the resource, or some unexpected
   * problem is encountered.
   */
  public SensorData getSensorData(SensorDataRef ref) throws NotAuthorizedException,
      ResourceNotFoundException, BadXmlException, MiscClientException {
    return getSensorData(UriUtils.getUriSuffix(ref.getSource()), ref.getTimestamp());
  }

  /**
   * Stores a SensorData object in the server.
   * 
   * @param data The SensorData object to be stored.
   * @return True if the SensorData could be stored, false otherwise.
   * @throws JAXBException If there are problems marshalling the object for upload.
   * @throws NotAuthorizedException If the client is not authorized to store the SensorData.
   * @throws ResourceNotFoundException If the source name referenced in the SensorData doesn't exist
   * on the server.
   * @throws BadXmlException If the server reports that the XML sent was bad, or the timestamp
   * specified was bad, or there was no XML, or the fields in the XML don't match the URI.
   * @throws OverwriteAttemptedException If there is already SensorData on the server with the given
   * timestamp.
   * @throws MiscClientException If the server indicates an unexpected problem has occurred.
   */
  public boolean storeSensorData(SensorData data) throws JAXBException, NotAuthorizedException,
      ResourceNotFoundException, BadXmlException, OverwriteAttemptedException, MiscClientException {
    Marshaller marshaller = sensorDataJAXB.createMarshaller();
    StringWriter writer = new StringWriter();
    if (data == null) {
      return false;
    }
    else {
      marshaller.marshal(data, writer);
    }
    Representation rep =
        new StringRepresentation(writer.toString(), MediaType.TEXT_XML, Language.ALL,
            CharacterSet.UTF_8);
    Response response =
        makeRequest(Method.PUT, Server.SOURCES_URI + "/" + UriUtils.getUriSuffix(data.getSource())
            + "/" + Server.SENSORDATA_URI + "/" + data.getTimestamp().toXMLFormat(), XML_MEDIA, rep);
    Status status = response.getStatus();
    if (status.equals(Status.CLIENT_ERROR_UNAUTHORIZED)) {
      // credentials were unacceptable to server
      throw new NotAuthorizedException(status);
    }
    if (status.equals(Status.CLIENT_ERROR_NOT_FOUND)) {
      // an unknown source name was specified
      throw new ResourceNotFoundException(status);
    }
    if (status.equals(Status.CLIENT_ERROR_BAD_REQUEST)) {
      // either bad timestamp provided in URI or bad XML in entity body
      throw new BadXmlException(status);
    }
    if (status.equals(Status.CLIENT_ERROR_CONFLICT)) {
      // client attempted to overwrite existing data
      throw new OverwriteAttemptedException(status);
    }
    if (status.isSuccess()) {
      return true;
    }
    else {
      // Some unexpected type of error received, so punt
      throw new MiscClientException(status);
    }
  }

  /**
   * Deletes a SensorData resource from the server.
   * 
   * @param source The name of the source containing the resource to be deleted.
   * @param timestamp The timestamp of the resource to be deleted.
   * @return True if the SensorData could be deleted, false otherwise.
   * @throws NotAuthorizedException If the client is not authorized to store the SensorData.
   * @throws BadXmlException If the server reports that the timestamp specified was bad
   * @throws ResourceNotFoundException If the source name doesn't exist on the server, or there is
   * no data with the given timestamp
   * @throws MiscClientException If the server indicates an unexpected problem has occurred.
   */
  public boolean deleteSensorData(String source, XMLGregorianCalendar timestamp)
      throws NotAuthorizedException, ResourceNotFoundException, BadXmlException,
      MiscClientException {
    Response response =
        makeRequest(Method.DELETE, Server.SOURCES_URI + "/" + source + "/" + Server.SENSORDATA_URI
            + "/" + timestamp.toXMLFormat(), XML_MEDIA, null);
    Status status = response.getStatus();

    if (status.equals(Status.CLIENT_ERROR_UNAUTHORIZED)) {
      // credentials were unacceptable to server
      throw new NotAuthorizedException(status);
    }
    if (status.equals(Status.CLIENT_ERROR_BAD_REQUEST)) {
      // bad timestamp provided in URI
      throw new BadXmlException(status);
    }
    if (status.equals(Status.CLIENT_ERROR_NOT_FOUND)) {
      // an unknown source name was specified, or timestamp could not be found
      throw new ResourceNotFoundException(status);
    }
    if (status.isSuccess()) {
      return true;
    }
    else {
      // Some unexpected type of error received, so punt
      throw new MiscClientException(status);
    }
  }

  /**
   * Returns the UserIndex containing all Users on the server. Note that only the admin user is
   * allowed to retrieve the UserIndex.
   * 
   * @return The UserIndex for the server.
   * @throws NotAuthorizedException If the client is not authorized to retrieve the user index.
   * @throws BadXmlException If error is encountered unmarshalling the XML from the server.
   * @throws MiscClientException If error is encountered retrieving the resource, or some unexpected
   * problem is encountered.
   */
  public UserIndex getUserIndex() throws NotAuthorizedException, BadXmlException,
      MiscClientException {
    Response response = makeRequest(Method.GET, Server.USERS_URI, XML_MEDIA, null);
    Status status = response.getStatus();

    if (status.equals(Status.CLIENT_ERROR_UNAUTHORIZED)) {
      // User credentials did not correspond to an admin
      throw new NotAuthorizedException(status);
    }
    if (status.isSuccess()) {
      try {
        String xmlString = response.getEntity().getText();
        // System.err.println("UserIndex in client: " + xmlString); // DEBUG
        Unmarshaller unmarshaller = userJAXB.createUnmarshaller();
        return (UserIndex) unmarshaller.unmarshal(new StringReader(xmlString));
      }
      catch (IOException e) {
        // Error getting the text from the entity body, bad news
        throw new MiscClientException(status, e);
      }
      catch (JAXBException e) {
        // Got some XML we can't parse
        throw new BadXmlException(status, e);
      }
    }
    else {
      // Some totally unexpected non-success status code, just throw generic client exception
      throw new MiscClientException(status);
    }
  }

  /**
   * Requests a List of all Users on the server. Note that only the admin user is allowed to
   * retrieve the the list of all Users. Currently this is just a convenience method, but if the
   * REST API is extended to support getting Users directly without going through a UserIndex, this
   * method will use that more efficient API call.
   * 
   * @return The List of Users accessible to the user.
   * @throws NotAuthorizedException If the client's credentials are bad.
   * @throws BadXmlException If error is encountered unmarshalling the XML from the server.
   * @throws MiscClientException If error is encountered retrieving the resource, or some unexpected
   * problem is encountered.
   */
  public List<User> getUsers() throws NotAuthorizedException, BadXmlException, MiscClientException {
    List<User> userList = new ArrayList<User>();
    UserIndex index = getUserIndex();
    for (UserRef ref : index.getUserRef()) {
      try {
        userList.add(getUser(ref));
      }
      catch (ResourceNotFoundException e) {
        // We got the SourceIndex already, so we know the source exists. this should never happen
        throw new MiscClientException("SourceRef from Source index had non-existent source name", e);
      }
    }
    return userList;
  }

  /**
   * Returns the named User resource.
   * 
   * @param username The username of the User resource to be retrieved.
   * @return The requested User.
   * @throws NotAuthorizedException If the client is not authorized to retrieve the User.
   * @throws ResourceNotFoundException If the username provided doesn't exist on the server.
   * @throws BadXmlException If error is encountered unmarshalling the XML from the server.
   * @throws MiscClientException If error is encountered retrieving the resource, or some unexpected
   * problem is encountered.
   */
  public User getUser(String username) throws NotAuthorizedException, ResourceNotFoundException,
      BadXmlException, MiscClientException {
    Response response = makeRequest(Method.GET, Server.USERS_URI + "/" + username, XML_MEDIA, null);
    Status status = response.getStatus();

    if (status.equals(Status.CLIENT_ERROR_UNAUTHORIZED)) {
      // credentials were unacceptable to server
      throw new NotAuthorizedException(status);
    }
    if (status.equals(Status.CLIENT_ERROR_NOT_FOUND)) {
      // an unknown username was specified
      throw new ResourceNotFoundException(status);
    }
    if (status.isSuccess()) {
      try {
        String xmlString = response.getEntity().getText();
        Unmarshaller unmarshaller = userJAXB.createUnmarshaller();
        return (User) unmarshaller.unmarshal(new StringReader(xmlString));
      }
      catch (IOException e) {
        // Error getting the text from the entity body, bad news
        throw new MiscClientException(status, e);
      }
      catch (JAXBException e) {
        // Got some XML we can't parse
        throw new BadXmlException(status, e);
      }
    }
    else {
      // Some totally unexpected non-success status code, just throw generic client exception
      throw new MiscClientException(status);
    }
  }

  /**
   * Returns the User resource specified by a UserRef.
   * 
   * @param ref The UserRef of the User resource to be retrieved.
   * @return The requested User.
   * @throws NotAuthorizedException If the client is not authorized to retrieve the User.
   * @throws ResourceNotFoundException If the username provided doesn't exist on the server.
   * @throws BadXmlException If error is encountered unmarshalling the XML from the server.
   * @throws MiscClientException If error is encountered retrieving the resource, or some unexpected
   * problem is encountered.
   */
  public User getUser(UserRef ref) throws NotAuthorizedException, ResourceNotFoundException,
      BadXmlException, MiscClientException {
    return getUser(ref.getEmail());
  }

  /**
   * Returns the SourceIndex containing all Sources accessible to the authenticated user on the
   * server. When looking to retrieve all the Source objects from the index, getSources is
   * preferable to this method.
   * 
   * @return The SourceIndex for the server.
   * @throws NotAuthorizedException If the client's credentials are bad.
   * @throws BadXmlException If error is encountered unmarshalling the XML from the server.
   * @throws MiscClientException If error is encountered retrieving the resource, or some unexpected
   * problem is encountered.
   * @see org.wattdepot.client.WattDepotClient#getSources getSources
   */
  public SourceIndex getSourceIndex() throws NotAuthorizedException, BadXmlException,
      MiscClientException {
    Response response = makeRequest(Method.GET, Server.SOURCES_URI, XML_MEDIA, null);
    Status status = response.getStatus();

    if (status.equals(Status.CLIENT_ERROR_UNAUTHORIZED)) {
      // User credentials are invalid
      throw new NotAuthorizedException(status);
    }
    if (status.isSuccess()) {
      try {
        String xmlString = response.getEntity().getText();
        Unmarshaller unmarshaller = sourceJAXB.createUnmarshaller();
        return (SourceIndex) unmarshaller.unmarshal(new StringReader(xmlString));
      }
      catch (IOException e) {
        // Error getting the text from the entity body, bad news
        throw new MiscClientException(status, e);
      }
      catch (JAXBException e) {
        // Got some XML we can't parse
        throw new BadXmlException(status, e);
      }
    }
    else {
      // Some totally unexpected non-success status code, just throw generic client exception
      throw new MiscClientException(status);
    }
  }

  /**
   * Requests a List of Sources representing all Sources accessible to the authenticated user on the
   * server. Uses the fetchAll parameter in the REST API, so only one HTTP request is made.
   * 
   * @return The List of Sources accessible to the user.
   * @throws NotAuthorizedException If the client's credentials are bad.
   * @throws BadXmlException If error is encountered unmarshalling the XML from the server.
   * @throws MiscClientException If error is encountered retrieving the resource, or some unexpected
   * problem is encountered.
   */
  public List<Source> getSources() throws NotAuthorizedException, BadXmlException,
      MiscClientException {
    String uri = Server.SOURCES_URI + "/?fetchAll=true";
    Response response = makeRequest(Method.GET, uri, XML_MEDIA, null);
    Status status = response.getStatus();

    if (status.equals(Status.CLIENT_ERROR_UNAUTHORIZED)) {
      // User credentials are invalid
      throw new NotAuthorizedException(status);
    }
    if (status.isSuccess()) {
      try {
        String xmlString = response.getEntity().getText();
        Unmarshaller unmarshaller = sourceJAXB.createUnmarshaller();
        Sources sources = (Sources) unmarshaller.unmarshal(new StringReader(xmlString));
        if (sources == null) {
          return null;
        }
        else {
          return sources.getSource();
        }
      }
      catch (IOException e) {
        // Error getting the text from the entity body, bad news
        throw new MiscClientException(status, e);
      }
      catch (JAXBException e) {
        // Got some XML we can't parse
        throw new BadXmlException(status, e);
      }
    }
    else {
      // Some totally unexpected non-success status code, just throw generic client exception
      throw new MiscClientException(status);
    }
  }

  /**
   * Requests the Source with the given name from the server.
   * 
   * @param source The name of the Source.
   * @return The Source.
   * @throws NotAuthorizedException If the client is not authorized to retrieve the Source.
   * @throws ResourceNotFoundException If the source name provided doesn't exist on the server.
   * @throws BadXmlException If error is encountered unmarshalling the XML from the server.
   * @throws MiscClientException If error is encountered retrieving the resource, or some unexpected
   * problem is encountered.
   */
  public Source getSource(String source) throws NotAuthorizedException, ResourceNotFoundException,
      BadXmlException, MiscClientException {
    Response response = makeRequest(Method.GET, Server.SOURCES_URI + "/" + source, XML_MEDIA, null);
    Status status = response.getStatus();

    if (status.equals(Status.CLIENT_ERROR_UNAUTHORIZED)) {
      // credentials were unacceptable to server
      throw new NotAuthorizedException(status);
    }
    if (status.equals(Status.CLIENT_ERROR_NOT_FOUND)) {
      // an unknown source name was specified
      throw new ResourceNotFoundException(status);
    }
    if (status.isSuccess()) {
      try {
        String xmlString = response.getEntity().getText();
        Unmarshaller unmarshaller = sourceJAXB.createUnmarshaller();
        return (Source) unmarshaller.unmarshal(new StringReader(xmlString));
      }
      catch (IOException e) {
        // Error getting the text from the entity body, bad news
        throw new MiscClientException(status, e);
      }
      catch (JAXBException e) {
        // Got some XML we can't parse
        throw new BadXmlException(status, e);
      }
    }
    else {
      // Some totally unexpected non-success status code, just throw generic client exception
      throw new MiscClientException(status);
    }
  }

  /**
   * Stores a Source resource in the server. If a Source resource with this name already exists, no
   * action is performed and the method throws a OverwriteAttemptedException, unless the overwrite
   * parameter is true, in which case the existing resource is overwritten.
   * 
   * @param source The Source resource to be stored.
   * @param overwrite If true, then overwrite the any existing resource with the given name. If
   * false, return false and do nothing if there is an existing resource with the given name.
   * @return True if the Source could be stored, false otherwise.
   * @throws JAXBException If there are problems marshalling the object for upload.
   * @throws NotAuthorizedException If the client is not authorized to store the Source.
   * @throws BadXmlException If the server reports that the XML sent was bad, or there was no XML,
   * or the fields in the XML don't match the URI.
   * @throws OverwriteAttemptedException If there is already a Source on the server with the same
   * name.
   * @throws MiscClientException If the server indicates an unexpected problem has occurred.
   */
  public boolean storeSource(Source source, boolean overwrite) throws JAXBException,
      NotAuthorizedException, BadXmlException, OverwriteAttemptedException, MiscClientException {
    Marshaller marshaller = sourceJAXB.createMarshaller();
    StringWriter writer = new StringWriter();
    if (source == null) {
      return false;
    }
    else {
      marshaller.marshal(source, writer);
    }
    Representation rep =
        new StringRepresentation(writer.toString(), MediaType.TEXT_XML, Language.ALL,
            CharacterSet.UTF_8);
    String overwriteFlag;
    if (overwrite) {
      overwriteFlag = "?overwrite=true";
    }
    else {
      overwriteFlag = "";
    }
    Response response =
        makeRequest(Method.PUT, Server.SOURCES_URI + "/" + source.getName() + overwriteFlag,
            XML_MEDIA, rep);
    Status status = response.getStatus();
    if (status.equals(Status.CLIENT_ERROR_UNAUTHORIZED)) {
      // credentials were unacceptable to server
      throw new NotAuthorizedException(status);
    }
    if (status.equals(Status.CLIENT_ERROR_BAD_REQUEST)) {
      // bad XML in entity body
      throw new BadXmlException(status);
    }
    if (status.equals(Status.CLIENT_ERROR_CONFLICT)) {
      // client attempted to overwrite existing data and didn't set overwrite to true
      throw new OverwriteAttemptedException(status);
    }
    if (status.isSuccess()) {
      return true;
    }
    else {
      // Some unexpected type of error received, so punt
      throw new MiscClientException(status);
    }
  }

  /**
   * Convenience method for retrieving a Source given its SourceRef.
   * 
   * @param ref The SourceRef of the desired Source.
   * @return The Source.
   * @throws NotAuthorizedException If the client is not authorized to retrieve the Source.
   * @throws ResourceNotFoundException If the source name provided doesn't exist on the server.
   * @throws BadXmlException If error is encountered unmarshalling the XML from the server.
   * @throws MiscClientException If error is encountered retrieving the resource, or some unexpected
   * problem is encountered.
   */
  public Source getSource(SourceRef ref) throws NotAuthorizedException, ResourceNotFoundException,
      BadXmlException, MiscClientException {
    return getSource(ref.getName());
  }

  /**
   * Requests the SourceSummary for the Source with the given name from the server.
   * 
   * @param source The name of the Source.
   * @return The SourceSummary.
   * @throws NotAuthorizedException If the client is not authorized to retrieve the SourceSummary.
   * @throws ResourceNotFoundException If the source name provided doesn't exist on the server.
   * @throws BadXmlException If error is encountered unmarshalling the XML from the server.
   * @throws MiscClientException If error is encountered retrieving the resource, or some unexpected
   * problem is encountered.
   */
  public SourceSummary getSourceSummary(String source) throws NotAuthorizedException,
      ResourceNotFoundException, BadXmlException, MiscClientException {
    Response response =
        makeRequest(Method.GET, Server.SOURCES_URI + "/" + source + "/" + Server.SUMMARY_URI,
            XML_MEDIA, null);
    Status status = response.getStatus();

    if (status.equals(Status.CLIENT_ERROR_UNAUTHORIZED)) {
      // credentials were unacceptable to server
      throw new NotAuthorizedException(status);
    }
    if (status.equals(Status.CLIENT_ERROR_NOT_FOUND)) {
      // an unknown source name was specified
      throw new ResourceNotFoundException(status);
    }
    if (status.isSuccess()) {
      try {
        String xmlString = response.getEntity().getText();
        Unmarshaller unmarshaller = sourceSummaryJAXB.createUnmarshaller();
        return (SourceSummary) unmarshaller.unmarshal(new StringReader(xmlString));
      }
      catch (IOException e) {
        // Error getting the text from the entity body, bad news
        throw new MiscClientException(status, e);
      }
      catch (JAXBException e) {
        // Got some XML we can't parse
        throw new BadXmlException(status, e);
      }
    }
    else {
      // Some totally unexpected non-success status code, just throw generic client exception
      throw new MiscClientException(status);
    }
  }

  /**
   * Attempts to make a snapshot of the database on the server. Requires admin privileges to
   * complete.
   * 
   * @return True if the snapshot could be created, false otherwise.
   * @throws NotAuthorizedException If the client is not authorized to create the snapshot.
   * @throws MiscClientException If the server rejected the snapshot request for some other reason.
   */
  public boolean makeSnapshot() throws NotAuthorizedException, MiscClientException {
    Response response =
        makeRequest(Method.PUT, Server.DATABASE_URI + "/" + "snapshot", XML_MEDIA, null);
    Status status = response.getStatus();

    if (status.equals(Status.CLIENT_ERROR_UNAUTHORIZED)) {
      // credentials were unacceptable to server, perhaps not admin?
      throw new NotAuthorizedException(status);
    }
    if (status.equals(Status.CLIENT_ERROR_BAD_REQUEST)) {
      // Unexpected, perhaps snapshot method not accepted?
      throw new MiscClientException(status);
    }
    if (status.equals(Status.SERVER_ERROR_INTERNAL)) {
      // Server had a problem creating the snapshot
      return false;
    }
    if (status.isSuccess()) {
      return true;
    }
    else {
      // Some totally unexpected non-success status code, just throw generic client exception
      throw new MiscClientException(status);
    }
  }

  /**
   * Retrieves the WattDepot URI used by this client. This is useful for creating resource objects
   * that have URIs in their fields (and thus need the WattDepot URI to construct those URIs).
   * 
   * @return The URI of the WattDepot server used by this client.
   */
  public String getWattDepotUri() {
    return wattDepotUri;
  }
}