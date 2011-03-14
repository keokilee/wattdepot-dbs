package org.wattdepot.resource.source;

import java.io.IOException;
import javax.xml.bind.JAXBException;
import org.restlet.Context;
import org.restlet.data.MediaType;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.data.Status;
import org.restlet.resource.Representation;
import org.restlet.resource.ResourceException;
import org.restlet.resource.Variant;
import org.wattdepot.resource.WattDepotResource;
import org.wattdepot.resource.source.jaxb.Source;

/**
 * Represents a source of sensor data, such a power meter. Sources can also be virtual, consisting
 * of an aggregation of other Sources.
 * 
 * @author Robert Brewer
 */

public class SourceResource extends WattDepotResource {

  /** fetchAll parameter from the URI, or else false if not found. */
  private boolean fetchAll = false;

  /** overwrite parameter from the URI, or else false if not found. */
  private boolean overwrite = false;

  /**
   * Creates a new SourceResource object with the provided parameters, and only a text/xml
   * representation.
   * 
   * @param context Restlet context for the resource
   * @param request Restlet request
   * @param response Restlet response
   */
  public SourceResource(Context context, Request request, Response response) {
    super(context, request, response);
    String fetchAllString = (String) request.getAttributes().get("fetchAll");
    this.fetchAll = "true".equalsIgnoreCase(fetchAllString);
    String overwriteString = (String) request.getAttributes().get("overwrite");
    this.overwrite = "true".equalsIgnoreCase(overwriteString);
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
    // If credentials are provided, they need to be valid
    if (!isAnonymous() && !validateCredentials()) {
      return null;
    }
    if (uriSource == null) {
      if (variant.getMediaType().equals(MediaType.TEXT_XML)) {
        // URI had no source parameter, which means the request is for the list of all sources
        try {
          if (isAnonymous()) {
            // anonymous users get only the public sources
            xmlString = getPublicSources(fetchAll);
          }
          else if (isAdminUser()) {
            // admin user can see all sources
            xmlString = getAllSources(fetchAll);
          }
          else {
            // Authenticated as some user
            xmlString = getOwnerSources(fetchAll);
          }
        }
        catch (JAXBException e) {
          setStatusInternalError(e);
          return null;
        }
        return getStringRepresentation(xmlString);
      }
      // Some MediaType other than text/xml requested
      else {
        return null;
      }
    }
    else {
      // First check if source in URI exists
      if (!validateKnownSource()) {
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
        try {
          xmlString = getSource();
        }
        catch (JAXBException e) {
          setStatusInternalError(e);
          return null;
        }
        return getStringRepresentation(xmlString);
      }
      // Some MediaType other than text/xml requested
      else {
        return null;
      }
    }
  }

  /**
   * Indicate the PUT method is supported.
   * 
   * @return True.
   */
  @Override
  public boolean allowPut() {
    return true;
  }

  /**
   * Implement the PUT method that creates a Source resource.
   * 
   * @param entity The entity to be posted.
   */
  @Override
  public void storeRepresentation(Representation entity) {
    // If credentials are provided, they need to be valid
    if (!validateCredentials()) {
      return;
    }
    // Get the payload.
    String entityString = null;
    try {
      entityString = entity.getText();
    }
    catch (IOException e) {
      setStatusMiscError("Bad or missing content");
      return;
    }
    Source source;
    String sourceName;
    // Try to make the XML payload into Source, return failure if this fails.
    if ((entityString == null) || ("".equals(entityString))) {
      setStatusMiscError("Entity body was empty");
      return;
    }
    try {
      source = makeSource(entityString);
      sourceName = source.getName();
    }
    catch (JAXBException e) {
      setStatusMiscError("Invalid Source representation: " + entityString);
      return;
    }
    // Return failure if the name of Source doesn't match the name given in URI
    if (!uriSource.equals(sourceName)) {
      setStatusMiscError("Soure Name field does not match source field in URI");
      return;
    }
    if (overwrite) {
      Source existingSource = dbManager.getSource(sourceName);
      // If source already exists, must be owner to overwrite
      if ((existingSource != null) && (!validateSourceOwnerOrAdmin())) {
        return;
      }
      if (dbManager.storeSource(source, overwrite)) {
        getResponse().setStatus(Status.SUCCESS_CREATED);
      }
      else {
        // all inputs have been validated by this point, so must be internal error
        setStatusInternalError(String.format("Unable to create Source named %s", uriSource));
        return;
      }
    }
    else {
      if (super.dbManager.getSource(uriSource) == null) {
        if (dbManager.storeSource(source)) {
          getResponse().setStatus(Status.SUCCESS_CREATED);
        }
        else {
          // all inputs have been validated by this point, so must be internal error
          setStatusInternalError(String.format("Unable to create Source named %s", uriSource));
          return;
        }
      }
      else {
        // if Source with given name already exists and not overwriting, then fail
        setStatusResourceOverwrite(uriSource);
        return;
      }
    }
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
