package org.wattdepot.tinker;

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
 * It's the machine that goes ping! http://www.youtube.com/watch?v=NcHdF1eHhgc&feature=channel_page
 * Based on the code from the Restlet first steps tutorial:
 * 
 * http://www.restlet.org/documentation/1.1/firstSteps
 * 
 * @author Robert Brewer
 */
public class PingResource extends Resource {

  /**
   * String to send as a response to the ping. 
   */
  public static final String HELLO_WORLD_TEXT = "Hello World!";
  
  /**
   * Creates a new PingResource object with the provided parameters, and only a text/plain
   * representation.
   * 
   * @param context Restlet context for the resource
   * @param request Restlet request
   * @param response Restlet response
   */
  public PingResource(Context context, Request request, Response response) {
    super(context, request, response);

    // This representation has only one type of representation.
    getVariants().add(new Variant(MediaType.TEXT_PLAIN));
  }

  /**
   * Returns a full representation for a given variant.
   * 
   * @param variant the requested variant of this representation
   * @return the representation of this resource, which is always "Hello World!" in plain text.
   * @throws ResourceException when the requested resource cannot be represented as requested.
   */
  @Override
  public Representation represent(Variant variant) throws ResourceException {
    return new StringRepresentation(HELLO_WORLD_TEXT, MediaType.TEXT_PLAIN);
  }

}
