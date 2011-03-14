package org.wattdepot.resource;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;
import java.util.ListIterator;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.datatype.XMLGregorianCalendar;
import org.restlet.Context;
import org.restlet.data.CharacterSet;
import org.restlet.data.Form;
import org.restlet.data.Language;
import org.restlet.data.MediaType;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.data.Status;
import org.restlet.resource.Resource;
import org.restlet.resource.StringRepresentation;
import org.restlet.resource.Variant;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.sensordata.jaxb.SensorDataIndex;
import org.wattdepot.resource.sensordata.jaxb.SensorDatas;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.resource.source.jaxb.SourceIndex;
import org.wattdepot.resource.source.jaxb.SourceRef;
import org.wattdepot.resource.source.jaxb.Sources;
import org.wattdepot.resource.source.summary.jaxb.SourceSummary;
import org.wattdepot.resource.user.jaxb.User;
import org.wattdepot.server.Server;
import org.wattdepot.server.db.DbBadIntervalException;
import org.wattdepot.server.db.DbManager;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * An abstract superclass for WattDepot resources, providing initialization and utility functions.
 * Portions of this code are adapted from http://hackystat-sensorbase-uh.googlecode.com/
 * 
 * @author Robert Brewer
 * @author Philip Johnson
 */
public class WattDepotResource extends Resource {

  /** Holds the class-wide User JAXBContext, which is thread-safe. */
  private static JAXBContext userJaxbContext;

  /** Holds the class-wide Source JAXBContext, which is thread-safe. */
  private static JAXBContext sourceJaxbContext;

  /** Holds the class-wide SourceSummary JAXBContext, which is thread-safe. */
  private static JAXBContext sourceSummaryJaxbContext;

  /** Holds the class-wide SensorData JAXBContext, which is thread-safe. */
  private static JAXBContext sensorDataJaxbContext;

  /** The server. */
  protected Server server;

  /** The DbManager associated with this server. */
  protected DbManager dbManager;

  /** Everyone generally wants to create one of these, so declare it here. */
  protected String responseMsg;

  /** The 'user' template parameter from the URI, or null if no parameter. */
  protected String uriUser;

  /** The 'source' template parameter from the URI, or null if no parameter. */
  protected String uriSource;

  /** The username retrieved from the ChallengeResponse, or null. */
  protected String authUsername = null;

  /** The password, retrieved from the ChallengeResponse, or null. */
  protected String authPassword = null;

  // JAXBContexts are thread safe, so we can share them across all instances and threads.
  // https://jaxb.dev.java.net/guide/Performance_and_thread_safety.html
  static {
    try {
      userJaxbContext =
          JAXBContext.newInstance(org.wattdepot.resource.user.jaxb.ObjectFactory.class);
      sourceJaxbContext =
          JAXBContext.newInstance(org.wattdepot.resource.source.jaxb.ObjectFactory.class);
      sourceSummaryJaxbContext =
          JAXBContext.newInstance(org.wattdepot.resource.source.summary.jaxb.ObjectFactory.class);
      sensorDataJaxbContext =
          JAXBContext.newInstance(org.wattdepot.resource.sensordata.jaxb.ObjectFactory.class);
    }
    catch (Exception e) {
      throw new RuntimeException("Couldn't create JAXB context instances.", e);
    }
  }

  /**
   * Provides the following representational variants: TEXT_XML.
   * 
   * @param context The context.
   * @param request The request object.
   * @param response The response object.
   */
  public WattDepotResource(Context context, Request request, Response response) {
    super(context, request, response);
    if (request.getChallengeResponse() != null) {
      this.authUsername = request.getChallengeResponse().getIdentifier();
      this.authPassword = new String(request.getChallengeResponse().getSecret());
    }
    this.server = (Server) getContext().getAttributes().get("WattDepotServer");
    this.dbManager = (DbManager) getContext().getAttributes().get("DbManager");
    this.uriUser = (String) request.getAttributes().get("user");
    this.uriSource = (String) request.getAttributes().get("source");

    // This resource has only one type of representation.
    getVariants().add(new Variant(MediaType.TEXT_XML));

    // Add Cross-Origin Resource Sharing header to all responses.
    // See https://developer.mozilla.org/En/HTTP_access_control for more details
    // TODO This should really be done at the individual resource level and should add the header
    // only for public resources, but this is a quick hack to support a JavaScript application.
    // Code from here: http://blog.arc90.com/2008/09/15/custom-http-response-headers-with-restlet/
    Form responseHeaders = (Form) getResponse().getAttributes().get("org.restlet.http.headers");
    if (responseHeaders == null) {
      responseHeaders = new Form();
      getResponse().getAttributes().put("org.restlet.http.headers", responseHeaders);
    }
    responseHeaders.add("Access-Control-Allow-Origin", "*");
  }

  /**
   * Creates and returns a new Restlet StringRepresentation built from xmlData. The xmlData will be
   * prefixed with a processing instruction indicating UTF-8 and version 1.0.
   * 
   * @param xmlData The XML data as a string.
   * @return A StringRepresentation of that xmlData.
   */
  public static StringRepresentation getStringRepresentation(String xmlData) {
    // StringBuilder builder = new StringBuilder(500);
    // builder.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    // builder.append(xmlData);
    // return new StringRepresentation(builder, MediaType.TEXT_XML, Language.ALL,
    // CharacterSet.UTF_8);
    return new StringRepresentation(xmlData, MediaType.TEXT_XML, Language.ALL, CharacterSet.UTF_8);
  }

  /**
   * Helper function that removes any newline characters from the supplied string and replaces them
   * with a blank line.
   * 
   * @param msg The msg whose newlines are to be removed.
   * @return The string without newlines.
   */
  private String removeNewLines(String msg) {
    return msg.replace(System.getProperty("line.separator"), " ");
  }

  /**
   * Returns the XML string containing the UserIndex with all defined Users.
   * 
   * @return The XML string providing an index to all current Users.
   * @throws JAXBException If there are problems mashalling the UserIndex
   */
  public String getUserIndex() throws JAXBException {
    Marshaller marshaller = userJaxbContext.createMarshaller();
    StringWriter writer = new StringWriter();

    marshaller.marshal(this.dbManager.getUsers(), writer);
    return writer.toString();
    // DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    // dbf.setNamespaceAware(true);
    // DocumentBuilder documentBuilder = dbf.newDocumentBuilder();
    // Document doc = documentBuilder.newDocument();
    // marshaller.marshal(this.dbManager.getUsers(), doc);
    // DOMSource domSource = new DOMSource(doc);
    // StringWriter writer = new StringWriter();
    // StreamResult result = new StreamResult(writer);
    // TransformerFactory tf = TransformerFactory.newInstance();
    // Transformer transformer = tf.newTransformer();
    // transformer.transform(domSource, result);
    // String xmlString = writer.toString();
    // // Now remove the processing instruction. This approach seems like a total hack.
    // xmlString = xmlString.substring(xmlString.indexOf('>') + 1);
    // return xmlString;
  }

  /**
   * Returns the XML string containing the User specified in the URI.
   * 
   * @param user The User resource to be marshalled into XML.
   * @return The XML string of URI-specified Users.
   * @throws JAXBException If there are problems marshalling the User.
   */
  public String getUser(User user) throws JAXBException {
    Marshaller marshaller = userJaxbContext.createMarshaller();
    StringWriter writer = new StringWriter();

    marshaller.marshal(user, writer);
    return writer.toString();
  }

  /**
   * Returns an XML string containing either a SourceIndex of SourceRefs or a Sources element for
   * all public Sources.
   * 
   * @param fetchAll True if a Sources element is desired, false for SourceIndex
   * @return The XML string with all the public sources.
   * @throws JAXBException If there are problems marshalling the SourceIndex.
   */
  public String getPublicSources(boolean fetchAll) throws JAXBException {
    Marshaller marshaller = sourceJaxbContext.createMarshaller();
    StringWriter writer = new StringWriter();

    if (fetchAll) {
      Sources sources = this.dbManager.getSources();
      List<Source> sourceList = sources.getSource();
      ListIterator<Source> iterator = sourceList.listIterator();
      // Use ListIterator to loop over all Sources, removing those that aren't public
      while (iterator.hasNext()) {
        if (!iterator.next().isPublic()) {
          iterator.remove();
        }
      }
      marshaller.marshal(sources, writer);
    }
    else {
      SourceIndex index = this.dbManager.getSourceIndex();
      List<SourceRef> sourceRefList = index.getSourceRef();
      ListIterator<SourceRef> iterator = sourceRefList.listIterator();
      // Use ListIterator to loop over all SourceRefs, removing those that aren't public
      while (iterator.hasNext()) {
        if (!iterator.next().isPublic()) {
          iterator.remove();
        }
      }
      marshaller.marshal(index, writer);
    }
    return writer.toString();
  }

  /**
   * Returns an XML string containing either a SourceIndex of SourceRefs or a Sources element for
   * all Sources (public and private). Only appropriate for an admin user.
   * 
   * @param fetchAll True if a Sources element is desired, false for SourceIndex
   * @return The XML string with all sources.
   * @throws JAXBException If there are problems marshalling the SourceIndex.
   */
  public String getAllSources(boolean fetchAll) throws JAXBException {
    Marshaller marshaller = sourceJaxbContext.createMarshaller();
    StringWriter writer = new StringWriter();

    if (fetchAll) {
      Sources sources = this.dbManager.getSources();
      marshaller.marshal(sources, writer);
    }
    else {
      SourceIndex index = this.dbManager.getSourceIndex();
      marshaller.marshal(index, writer);
    }
    return writer.toString();
  }

  /**
   * Returns an XML string containing either a SourceIndex of SourceRefs or a Sources element for
   * all public Sources and any sources owned by the current authenticated user.
   * 
   * @param fetchAll True if a Sources element is desired, false for SourceIndex
   * @return The XML string with all sources.
   * @throws JAXBException If there are problems marshalling the SourceIndex.
   */
  public String getOwnerSources(boolean fetchAll) throws JAXBException {
    Marshaller marshaller = sourceJaxbContext.createMarshaller();
    StringWriter writer = new StringWriter();

    if (fetchAll) {
      Sources sources = this.dbManager.getSources();
      List<Source> sourceList = sources.getSource();
      ListIterator<Source> iterator = sourceList.listIterator();
      // Use ListIterator to loop over all Sources, removing those that aren't public
      while (iterator.hasNext()) {
        Source source = iterator.next();
        if (!source.isPublic() && !isSourceOwner(source.getName())) {
          iterator.remove();
        }
      }
      marshaller.marshal(sources, writer);
    }
    else {
      SourceIndex index = this.dbManager.getSourceIndex();
      List<SourceRef> sourceList = index.getSourceRef();
      ListIterator<SourceRef> iterator = sourceList.listIterator();
      // Use ListIterator to loop over all SourceRefs, removing those that aren't public or owned by
      // current user
      while (iterator.hasNext()) {
        SourceRef ref = iterator.next();
        if (!ref.isPublic() && !isSourceOwner(ref.getName())) {
          iterator.remove();
        }
      }
      marshaller.marshal(index, writer);
    }
    return writer.toString();
  }

  /**
   * Returns the XML string containing the Source specified in the URI.
   * 
   * @return The XML string of URI-specified Source.
   * @throws JAXBException If there are problems marshalling the Source.
   */
  public String getSource() throws JAXBException {
    Source source = this.dbManager.getSource(uriSource);
    Marshaller marshaller = sourceJaxbContext.createMarshaller();
    StringWriter writer = new StringWriter();

    marshaller.marshal(source, writer);
    return writer.toString();
  }

  /**
   * Returns the XML string containing the SourceSummary specified in the URI.
   * 
   * @return The XML string for the SourceSummar of the URI-specified Source.
   * @throws JAXBException If there are problems marshalling the SourceSummary.
   */
  public String getSourceSummary() throws JAXBException {
    SourceSummary summary = this.dbManager.getSourceSummary(uriSource);
    Marshaller marshaller = sourceSummaryJaxbContext.createMarshaller();
    StringWriter writer = new StringWriter();

    marshaller.marshal(summary, writer);
    return writer.toString();
  }

  /**
   * Takes a String encoding of a Source in XML format and converts it to an instance.
   * 
   * @param xmlString The XML string representing a Source.
   * @return The corresponding Source instance.
   * @throws JAXBException If problems occur during unmarshalling.
   */
  public Source makeSource(String xmlString) throws JAXBException {
    Unmarshaller unmarshaller = sourceJaxbContext.createUnmarshaller();
    return (Source) unmarshaller.unmarshal(new StringReader(xmlString));
  }

  /**
   * Returns an XML string representation of a SensorDataIndex containing all the SensorData for the
   * Source name given in the URI, or null if the named Source doesn't exist.
   * 
   * @return The XML string representing the requested SensorDataIndex, or null if source name is
   * unknown.
   * @throws JAXBException If there are problems mashalling the SensorDataIndex.
   */
  public String getSensorDataIndex() throws JAXBException {
    Marshaller marshaller = sensorDataJaxbContext.createMarshaller();
    // use line breaks and indentation in XML output so it is more readable. Might want to turn
    // this off after development gets more stable, to save bandwidth.
    marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
    StringWriter writer = new StringWriter();
    SensorDataIndex index = this.dbManager.getSensorDataIndex(this.uriSource);
    if (index == null) {
      return null;
    }
    else {
      marshaller.marshal(index, writer);
      return writer.toString();
    }
  }

  /**
   * Returns the XML string containing the SensorData for the Source name given in the URI and the
   * given timestamp, or null if no SensorData exists.
   * 
   * @param timestamp The timestamp requested.
   * @return The XML string representing the requested SensorData, or null if it cannot be found.
   * @throws JAXBException If there are problems mashalling the SensorData.
   */
  public String getSensorData(XMLGregorianCalendar timestamp) throws JAXBException {
    Marshaller marshaller = sensorDataJaxbContext.createMarshaller();
    StringWriter writer = new StringWriter();
    SensorData data = this.dbManager.getSensorData(this.uriSource, timestamp);
    if (data == null) {
      return null;
    }
    else {
      marshaller.marshal(data, writer);
      return writer.toString();
    }
  }

  /**
   * Returns the XML string containing the latest SensorData for the Source name given in the URI,
   * or null if no SensorData exists.
   * 
   * @return The XML string representing the latest SensorData, or null if it cannot be found.
   * @throws JAXBException If there are problems mashalling the SensorData.
   */
  public String getLatestSensorData() throws JAXBException {
    Marshaller marshaller = sensorDataJaxbContext.createMarshaller();
    StringWriter writer = new StringWriter();
    SensorData data = this.dbManager.getLatestSensorData(this.uriSource);
    if (data == null) {
      return null;
    }
    else {
      marshaller.marshal(data, writer);
      return writer.toString();
    }
  }

  /**
   * Returns an XML string representation of a SensorDataIndex containing all the SensorData for the
   * Source name given in the URI between the provided start and end times, or null if the named
   * Source doesn't exist.
   * 
   * @param startTime The start time requested.
   * @param endTime The end time requested.
   * @return The XML string representing the requested SensorDataIndex, or null if source name is
   * unknown.
   * @throws JAXBException If there are problems mashalling the SensorDataIndex.
   * @throws DbBadIntervalException If the start time is later than the end time.
   */
  public String getSensorDataIndex(XMLGregorianCalendar startTime, XMLGregorianCalendar endTime)
      throws JAXBException, DbBadIntervalException {
    Marshaller marshaller = sensorDataJaxbContext.createMarshaller();
    StringWriter writer = new StringWriter();
    SensorDataIndex index = this.dbManager.getSensorDataIndex(this.uriSource, startTime, endTime);
    if (index == null) {
      return null;
    }
    else {
      marshaller.marshal(index, writer);
      return writer.toString();
    }
  }

  /**
   * Returns an XML string representation of a SensorDatas object containing all the SensorData for
   * the Source name given in the URI between the provided start and end times, or null if the named
   * Source doesn't exist.
   * 
   * @param startTime The start time requested.
   * @param endTime The end time requested.
   * @return The XML string representing the requested SensorDatas object, or null if source name is
   * unknown.
   * @throws JAXBException If there are problems mashalling the SensorDataIndex.
   * @throws DbBadIntervalException If the start time is later than the end time.
   */
  public String getSensorDatas(XMLGregorianCalendar startTime, XMLGregorianCalendar endTime)
      throws JAXBException, DbBadIntervalException {
    Marshaller marshaller = sensorDataJaxbContext.createMarshaller();
    StringWriter writer = new StringWriter();
    SensorDatas datas = this.dbManager.getSensorDatas(this.uriSource, startTime, endTime);
    if (datas == null) {
      return null;
    }
    else {
      marshaller.marshal(datas, writer);
      return writer.toString();
    }
  }

  /**
   * Takes a String encoding of a SensorData in XML format and converts it to an instance.
   * 
   * @param xmlString The XML string representing a SensorData.
   * @return The corresponding SensorData instance.
   * @throws JAXBException If problems occur during unmarshalling.
   */
  public SensorData makeSensorData(String xmlString) throws JAXBException {
    Unmarshaller unmarshaller = sensorDataJaxbContext.createUnmarshaller();
    return (SensorData) unmarshaller.unmarshal(new StringReader(xmlString));
  }

  /**
   * Returns the XML string containing the power in SensorData format for the Source name given in
   * the URI and the given timestamp, or null if no power data exists.
   * 
   * @param timestamp The timestamp requested.
   * @return The XML string representing the requested power in SensorData format, or null if it
   * cannot be found/calculated.
   * @throws JAXBException If there are problems mashalling the SensorData.
   */
  public String getPower(XMLGregorianCalendar timestamp) throws JAXBException {
    Marshaller marshaller = sensorDataJaxbContext.createMarshaller();
    StringWriter writer = new StringWriter();
    SensorData powerData = this.dbManager.getPower(this.uriSource, timestamp);
    if (powerData == null) {
      return null;
    }
    else {
      marshaller.marshal(powerData, writer);
      return writer.toString();
    }
  }

  /**
   * Returns the XML string containing the energy in SensorData format for the Source name given in
   * the URI over the range of time between startTime and endTime, or null if no energy data exists.
   * 
   * @param startTime The start of the range requested.
   * @param endTime The start of the range requested.
   * @param interval The sampling interval requested.
   * @return The XML string representing the requested energy in SensorData format, or null if it
   * cannot be found/calculated.
   * @throws JAXBException If there are problems mashalling the SensorData.
   */
  public String getEnergy(XMLGregorianCalendar startTime, XMLGregorianCalendar endTime, int interval)
      throws JAXBException {
    Marshaller marshaller = sensorDataJaxbContext.createMarshaller();
    StringWriter writer = new StringWriter();
    SensorData energyData = null;
    long rangeLength = Tstamp.diff(startTime, endTime);
    long minutesToMilliseconds = 60L * 1000L;

    if (interval < 0) {
      setStatusBadSamplingInterval(Integer.toString(interval));
      // TODO BOGUS, should throw an exception so EnergyResource can distinguish between problems
      return null;
    }
    else if ((interval * minutesToMilliseconds) > rangeLength) {
      setStatusBadSamplingInterval(Integer.toString(interval));
      // TODO BOGUS, should throw an exception so EnergyResource can distinguish between problems
      return null;
    }
    energyData = this.dbManager.getEnergy(this.uriSource, startTime, endTime, interval);
    if (energyData == null) {
      return null;
    }
    else {
      marshaller.marshal(energyData, writer);
      return writer.toString();
    }
  }

  /**
   * Returns the XML string containing the carbon in SensorData format for the Source name given in
   * the URI over the range of time between startTime and endTime, or null if no carbon data exists.
   * 
   * @param startTime The start of the range requested.
   * @param endTime The start of the range requested.
   * @param interval The sampling interval requested.
   * @return The XML string representing the requested carbon in SensorData format, or null if it
   * cannot be found/calculated.
   * @throws JAXBException If there are problems mashalling the SensorData.
   */
  public String getCarbon(XMLGregorianCalendar startTime, XMLGregorianCalendar endTime, int interval)
      throws JAXBException {
    Marshaller marshaller = sensorDataJaxbContext.createMarshaller();
    StringWriter writer = new StringWriter();
    SensorData carbonData = null;
    long rangeLength = Tstamp.diff(startTime, endTime);
    long minutesToMilliseconds = 60L * 1000L;

    if (interval < 0) {
      setStatusBadSamplingInterval(Integer.toString(interval));
      // TODO BOGUS, should throw an exception so EnergyResource can distinguish between problems
      return null;
    }
    else if ((interval * minutesToMilliseconds) > rangeLength) {
      setStatusBadSamplingInterval(Integer.toString(interval));
      // TODO BOGUS, should throw an exception so EnergyResource can distinguish between problems
      return null;
    }
    carbonData = this.dbManager.getCarbon(this.uriSource, startTime, endTime, interval);
    if (carbonData == null) {
      return null;
    }
    else {
      marshaller.marshal(carbonData, writer);
      return writer.toString();
    }
  }

  /**
   * Returns true if the source name present in the URI is valid, i.e. it exists in the database.
   * Otherwise sets the Response status and returns false.
   * 
   * @return True if the named Source exists in the database, false otherwise.
   */
  public boolean validateKnownSource() {
    // a Source is valid if we can retrieve it from the database
    if (dbManager.getSource(uriSource) == null) {
      // Might want to use a generic response message so as to not leak information.
      setStatusUnknownSource();
      return false;
    }
    else {
      return true;
    }
  }

  /**
   * Determines whether the credentials provided in the HTTP request indicate anonymous access or
   * not.
   * 
   * @return true if anonymous access (no credentials), false otherwise.
   */
  public boolean isAnonymous() {
    return (this.authUsername == null) && (this.authPassword == null);
  }

  /**
   * Determines whether the credentials provided in the HTTP request match the values for the User
   * in the database. Otherwise sets the Response status and returns false. Note that this method
   * will fail if there are no credentials available (client attempting anonymous access).
   * 
   * @return True if the credentials match, false if they don't or the user doesn't exist
   */
  public boolean validateCredentials() {
    User user = dbManager.getUser(authUsername);
    if (user == null) {
      setStatusBadCredentials();
      return false;
    }
    else {
      if (user.getEmail().equals(authUsername) && user.getPassword().equals(authPassword)) {
        return true;
      }
      else {
        setStatusBadCredentials();
        return false;
      }
    }
  }

  /**
   * Returns true if the username provided in the HTTP request is an admin user. Note, does not
   * check whether the credentials are valid (i.e. the password matches)!
   * 
   * @return True if the username in the credentials is an administrator, false otherwise.
   */
  public boolean isAdminUser() {
    User user = dbManager.getUser(authUsername);
    if (user == null) {
      return false;
    }
    return user.isAdmin();
  }

  /**
   * Returns true if the username provided in the HTTP request is the owner of the Source in the
   * URI. Note, does not check whether the credentials are valid (i.e. the password matches)!
   * 
   * @return True if the username in the credentials is the owner of the Source in the URI, false
   * otherwise.
   */
  public boolean isSourceOwner() {
    return isSourceOwner(uriSource);
  }

  /**
   * Returns true if the username provided in the HTTP request is the owner of the given source name
   * Note, does not check whether the credentials are valid (i.e. the password matches)!
   * 
   * @param sourceName The name of the source to check the username against.
   * @return True if the username in the credentials is the owner of the source name provided, false
   * otherwise.
   */
  public boolean isSourceOwner(String sourceName) {
    Source source = dbManager.getSource(sourceName);
    User user = dbManager.getUser(authUsername);
    if ((source == null) || (user == null)) {
      return false;
    }
    else {
      // Check if the URI of the authenticated user matches the URI of the owner for the source
      // parameter in the URI
      return user.toUri(this.server).equals(source.getOwner());
    }
  }

  /**
   * Returns true if the the authenticated user is the owner of the source name present in the URI,
   * or if the authenticated user is an administrator (the SourceOwner access control level
   * discussed in the REST API). Otherwise sets the Response status and returns false.
   * 
   * @return True if the current user owns the source in the URI or if the current user is an
   * administrator, false otherwise.
   */
  public boolean validateSourceOwnerOrAdmin() {
    if (isSourceOwner() || isAdminUser()) {
      return true;
    }
    else {
      // Sending 401, which leaks information but makes debugging easier. Maybe switch to 404
      // later??
      this.responseMsg = ResponseMessage.notSourceOwner(this, authUsername, uriSource);
      getResponse().setStatus(Status.CLIENT_ERROR_UNAUTHORIZED, removeNewLines(this.responseMsg));
      return false;
    }
  }

  /**
   * Called when an exception is caught while processing a request. Just sets the response code.
   * 
   * @param e The exception that was caught.
   */
  protected void setStatusInternalError(Exception e) {
    this.responseMsg = ResponseMessage.internalError(this, this.getLogger(), e);
    getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, removeNewLines(this.responseMsg));
  }

  /**
   * Called when an unknown error occurs while processing a request. Just sets the response code.
   * 
   * @param message The reason for the error.
   */
  protected void setStatusInternalError(String message) {
    this.responseMsg = ResponseMessage.internalError(this, this.getLogger(), message);
    getResponse().setStatus(Status.SERVER_ERROR_INTERNAL, removeNewLines(this.responseMsg));
  }

  /**
   * Called when a bad timestamp is found while processing a request. Just sets the response code.
   * 
   * @param timestamp The timestamp that could not be parsed.
   */
  protected void setStatusBadTimestamp(String timestamp) {
    this.responseMsg = ResponseMessage.badTimestamp(this, timestamp);
    getResponse().setStatus(Status.CLIENT_ERROR_BAD_REQUEST, removeNewLines(this.responseMsg));
  }

  /**
   * Called when a bad sampling interval is found while processing a request. Just sets the response
   * code.
   * 
   * @param interval The sampling interval that could not be parsed.
   */
  protected void setStatusBadSamplingInterval(String interval) {
    this.responseMsg = ResponseMessage.badSamplingInterval(this, interval);
    getResponse().setStatus(Status.CLIENT_ERROR_BAD_REQUEST, removeNewLines(this.responseMsg));
  }

  /**
   * Called when an bad interval (startTime > endTime) is encountered while processing a request.
   * Just sets the response code.
   * 
   * @param startTime The start time provided.
   * @param endTime The end time provided.
   */
  protected void setStatusBadInterval(String startTime, String endTime) {
    this.responseMsg = ResponseMessage.badInterval(this, startTime, endTime);
    getResponse().setStatus(Status.CLIENT_ERROR_BAD_REQUEST, removeNewLines(this.responseMsg));
  }

  /**
   * Called when a timestamp has been requested that does not exist. Just sets the response code.
   * 
   * @param timestamp The timestamp provided.
   */
  protected void setStatusTimestampNotFound(String timestamp) {
    this.responseMsg = ResponseMessage.timestampNotFound(this, this.uriSource, timestamp);
    getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND, removeNewLines(this.responseMsg));
  }

  /**
   * Called when a timestamp has been requested that does not exist. Just sets the response code.
   */
  protected void setStatusSourceLacksSensorData() {
    this.responseMsg = ResponseMessage.sourceLacksSensorData(this, this.uriSource);
    getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND, removeNewLines(this.responseMsg));
  }

  /**
   * Called when range specified by a startTime & endTime extends beyond a Source's sensor data
   * while processing a request. Just sets the response code.
   * 
   * @param startTime The start time provided.
   * @param endTime The end time provided.
   */
  protected void setStatusBadRange(String startTime, String endTime) {
    this.responseMsg = ResponseMessage.badRange(this, startTime, endTime);
    getResponse().setStatus(Status.CLIENT_ERROR_BAD_REQUEST, removeNewLines(this.responseMsg));
  }

  /**
   * Called if the URI contains an unknown Source. Just sets the response code. Might want to use a
   * generic response message so as to not leak information.
   */
  protected void setStatusUnknownSource() {
    this.responseMsg = ResponseMessage.unknownSource(this, this.uriSource);
    getResponse().setStatus(Status.CLIENT_ERROR_NOT_FOUND, removeNewLines(this.responseMsg));
  }

  /**
   * Called if a request is made that would overwrite an existing resource. Just sets the response
   * code.
   * 
   * @param resource The instance of the resource that was attempting to overwrite (ex. a
   * timestamp).
   */
  protected void setStatusResourceOverwrite(String resource) {
    this.responseMsg = ResponseMessage.resourceOverwrite(this, resource);
    getResponse().setStatus(Status.CLIENT_ERROR_CONFLICT, removeNewLines(this.responseMsg));
  }

  /**
   * Called if the credentials provided are invalid. Just sets the response code.
   */
  protected void setStatusBadCredentials() {
    this.responseMsg = ResponseMessage.badCredentials(this);
    // Sending 401, which leaks information but makes debugging easier. Maybe switch to 404
    // later??
    getResponse().setStatus(Status.CLIENT_ERROR_UNAUTHORIZED, removeNewLines(this.responseMsg));
  }

  /**
   * Called when a miscellaneous "one off" error is caught during processing.
   * 
   * @param msg A description of the error.
   */
  protected void setStatusMiscError(String msg) {
    this.responseMsg = ResponseMessage.miscError(this, msg);
    getResponse().setStatus(Status.CLIENT_ERROR_BAD_REQUEST, removeNewLines(this.responseMsg));
  }
}
