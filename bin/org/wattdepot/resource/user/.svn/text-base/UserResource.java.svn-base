package org.wattdepot.resource.user;

import javax.xml.bind.JAXBException;
import org.restlet.Context;
import org.restlet.data.MediaType;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.resource.Representation;
import org.restlet.resource.ResourceException;
import org.restlet.resource.Variant;
import org.wattdepot.resource.WattDepotResource;
import org.wattdepot.resource.user.jaxb.User;

/**
 * Represents a particular user of WattDepot. While most access to WattDepot can be done
 * anonymously, creating Sources and Sensor Data must be done by a registered User. The information
 * in the User resource is also used to authenticate and authorize access to the entire service via
 * HTTP authentication.
 * 
 * @author Robert Brewer
 */

public class UserResource extends WattDepotResource {
  /**
   * Creates a new UsersResource object with the provided parameters.
   * 
   * @param context Restlet context for the resource
   * @param request Restlet request
   * @param response Restlet response
   */
  public UserResource(Context context, Request request, Response response) {
    super(context, request, response);
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
    if (variant.getMediaType().equals(MediaType.TEXT_XML)) {
      // If credentials are provided, they need to be valid
      if (!isAnonymous() && !validateCredentials()) {
        return null;
      }
      if (uriUser == null) {
        // URI had no user parameter, which means the request is for the list of all users
        try {
          if (isAdminUser()) {
            // admin user can see all users
            xmlString = getUserIndex();
            return getStringRepresentation(xmlString);
          }
          else {
            // Authenticated as some user
            setStatusBadCredentials();
            return null;
          }
        }
        catch (JAXBException e) {
          setStatusInternalError(e);
          return null;
        }
      }
      else {
        User user = dbManager.getUser(authUsername);
        if (user == null) {
          // Note that technically this doesn't represent bad credentials, it is a request for a
          // user that doesn't exist. However, if the user doesn't exist, then any credentials
          // provided will be invalid. If we returned a different status code (like 404) that
          // would leak information about what users exist, which is bad.
          setStatusBadCredentials();
          return null;
        }
        else {
          if (user.getEmail().equals(authUsername) && user.getPassword().equals(authPassword)) {
            try {
              xmlString = getUser(user);
              return getStringRepresentation(xmlString);
            }
            catch (JAXBException e) {
              setStatusInternalError(e);
              return null;
            }
          }
          else {
            setStatusBadCredentials();
            return null;
          }
        }
      }
    }
    // Some MediaType other than text/xml requested
    else {
      return null;
    }
  }

  /**
   * Indicate the PUT method is not supported, until I have time to implement it.
   * 
   * @return False.
   */
  @Override
  public boolean allowPut() {
    return false;
  }

  /**
   * Indicate the DELETE method is not supported, until I have time to implement it.
   * 
   * @return False.
   */
  @Override
  public boolean allowDelete() {
    return false;
  }
}
