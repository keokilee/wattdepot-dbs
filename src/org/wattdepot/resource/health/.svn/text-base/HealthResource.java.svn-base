package org.wattdepot.resource.health;

import org.restlet.Context;
import org.restlet.data.MediaType;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.resource.Representation;
import org.restlet.resource.Resource;
import org.restlet.resource.ResourceException;
import org.restlet.resource.StringRepresentation;
import org.restlet.resource.Variant;

/**
 * Represents the health of the WattDepot service. It is provided as a way to determine if the
 * WattDepot service is up and functioning normally (like a "ping" resource).
 * 
 * @author Robert Brewer
 */
public class HealthResource extends Resource {

  /**
   * String to send as a response to the health request. 
   */
  protected static final String HEALTH_MESSAGE_TEXT = "WattDepot is alive.";
  
  /**
   * Creates a new HealthResource object with the provided parameters, and only a text/plain
   * representation.
   * 
   * @param context Restlet context for the resource
   * @param request Restlet request
   * @param response Restlet response
   */
  public HealthResource(Context context, Request request, Response response) {
    super(context, request, response);

    // This resource has only one type of representation.
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
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
    return new StringRepresentation(HEALTH_MESSAGE_TEXT, MediaType.TEXT_PLAIN);
  }

}
