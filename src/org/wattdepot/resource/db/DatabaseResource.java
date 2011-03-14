/**
 * 
 */
package org.wattdepot.resource.db;

import org.restlet.Context;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.data.Status;
import org.restlet.resource.Representation;
import org.wattdepot.resource.WattDepotResource;

/**
 * The Database resource is used by an administrator to perform certain actions on the database that
 * persists resources in WattDepot.
 * 
 * @author Robert Brewer
 */
public class DatabaseResource extends WattDepotResource {

  /** Contains the database method desired. */
  protected String methodString;

  /**
   * Creates a new DatabaseResource object with the provided parameters.
   * 
   * @param context Restlet context for the resource
   * @param request Restlet request
   * @param response Restlet response
   */
  public DatabaseResource(Context context, Request request, Response response) {
    super(context, request, response);
    // Right now, this resource is write-only. This prevents GETs
    this.setReadable(false);
    this.methodString = (String) request.getAttributes().get("method");
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
   * Implement the PUT method that executes the method provided.
   * 
   * @param entity The entity to be posted.
   */
  @Override
  public void storeRepresentation(Representation entity) {
    // If credentials are provided, they need to be valid
    if (validateCredentials()) {
      if (isAdminUser()) {
        if ("snapshot".equalsIgnoreCase(this.methodString)) {
          if (super.dbManager.makeSnapshot()) {
            getResponse().setStatus(Status.SUCCESS_CREATED);
          }
          else {
            // all inputs have been validated by this point, so must be internal error
            setStatusInternalError("Unable to create database snapshot");
            return;
          }
        }
        else {
          // Unknown method requested, return error
          setStatusMiscError("Bad method passed to Database resource");
        }
      }
      else {
        setStatusBadCredentials();
        return;
      }
    }
    else {
      return;
    }
  }
}
