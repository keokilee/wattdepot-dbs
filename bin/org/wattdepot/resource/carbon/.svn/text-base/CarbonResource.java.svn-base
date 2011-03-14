package org.wattdepot.resource.carbon;

import javax.xml.bind.JAXBException;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.util.tstamp.Tstamp;
import org.restlet.Context;
import org.restlet.data.MediaType;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.resource.Representation;
import org.restlet.resource.ResourceException;
import org.restlet.resource.Variant;
import org.wattdepot.resource.WattDepotResource;
import org.wattdepot.resource.source.jaxb.Source;

/**
 * Represents carbon emitted determined by sensor data from a particular source.
 * 
 * @author Robert Brewer
 */

public class CarbonResource extends WattDepotResource {

  /** To be retrieved from the URI, or else null if not found. */
  private String startTime, endTime, interval;

  /**
   * Creates a new CarbonResource object with the provided parameters, and only a text/xml
   * representation.
   * 
   * @param context Restlet context for the resource
   * @param request Restlet request
   * @param response Restlet response
   */
  public CarbonResource(Context context, Request request, Response response) {
    super(context, request, response);
    this.startTime = (String) request.getAttributes().get("startTime");
    this.endTime = (String) request.getAttributes().get("endTime");
    this.interval = (String) request.getAttributes().get("samplingInterval");
  }

  /**
   * Returns a full representation for a given variant.
   * 
   * @param variant the requested variant of this representation
   * @return the representation of this resource
   * @throws ResourceException when the requested resource cannot be represented as requested.
   */
  @Override
  public Representation represent(Variant variant) throws ResourceException {
    String xmlString;
    // First check if source in URI exists
    if (!validateKnownSource()) {
      return null;
    }
    // If credentials are provided, they need to be valid
    if (!isAnonymous() && !validateCredentials()) {
      return null;
    }
    Source source = dbManager.getSource(uriSource);
    // If source is private, check if current user is allowed to view
    if ((!source.isPublic()) && (!validateSourceOwnerOrAdmin())) {
      return null;
    }
    // If we make it here, we're all clear to send the XML: either source is public or source is
    // private but user is authorized to GET.
    if (variant.getMediaType().equals(MediaType.TEXT_XML)) {
      if ((this.startTime == null) || (this.endTime == null)) {
        // Some bad combination of options, so just fail
        setStatusMiscError("Request could not be understood.");
        return null;
      }
      else {
        XMLGregorianCalendar startObj = null, endObj = null;
        int intervalMinutes = 0;
        // check if start timestamp is OK
        try {
          startObj = Tstamp.makeTimestamp(this.startTime);
        }
        catch (Exception e) {
          setStatusBadTimestamp(this.startTime);
          return null;
        }
        // check if end timestamp is OK
        try {
          endObj = Tstamp.makeTimestamp(this.endTime);
        }
        catch (Exception e) {
          setStatusBadTimestamp(this.endTime);
          return null;
        }

        if (this.interval != null) {
          // convert to integer
          try {
            intervalMinutes = Integer.valueOf(this.interval);
          }
          catch (NumberFormatException e) {
            setStatusBadSamplingInterval(this.interval);
          }
        }
        // build XML string
        try {
          xmlString = getCarbon(startObj, endObj, intervalMinutes);
          // if we get a null, then there is no SensorData for this range
          if (xmlString == null) {
            setStatusBadRange(startObj.toString(), endObj.toString());
            return null;
          }
          return super.getStringRepresentation(xmlString);
        }
        catch (JAXBException e) {
          setStatusInternalError(e);
          return null;
        }
      }
    }
    // Some MediaType other than text/xml requested
    else {
      return null;
    }
  }

  /**
   * Indicate the DELETE method is not supported.
   * 
   * @return false.
   */
  @Override
  public boolean allowDelete() {
    return false;
  }

  /**
   * Indicate the PUT method is not supported.
   * 
   * @return false.
   */
  @Override
  public boolean allowPut() {
    return false;
  }
}
