package org.wattdepot.resource;

import java.util.logging.Logger;
import org.wattdepot.util.StackTrace;

/**
 * Provides standardized strings and formatting for response codes. This class is intended to make
 * error reporting more uniform and informative. A good error message will always include an
 * explanation for why the operation failed, and what the requested operation was. Portions of this
 * code are adapted from http://hackystat-sensorbase-uh.googlecode.com/
 * 
 * @author Philip Johnson
 * @author Robert Brewer
 */
public class ResponseMessage {

  // /**
  // * The error message for requests that only the admin can handle.
  // *
  // * @param resource The resource associated with this request.
  // * @return A string describing the problem.
  // */
  // static String adminOnly(WattDepotResource resource) {
  // return String.format("Request requires administrator privileges:%n  Request: %s %s", resource
  // .getRequest().getMethod().getName(), resource.getRequest().getResourceRef().toString());
  // }
  //
  // /**
  // * The error message for requests where the authorized user must be the same as the user in the
  // * URI string, or the authorized use is the admin (and then the user in the URI string can be
  // * anyone).
  // *
  // * @param resource The resource associated with this request.
  // * @param authUser The authorized user.
  // * @param uriUser The user in the URI string.
  // * @return A string describing the problem.
  // */
  // static String adminOrAuthUserOnly(WattDepotResource resource, String authUser, String uriUser)
  // {
  // return String.format("Request requires authorized user (%s) to be the same user as the "
  // + "URL user (%s):%n  Request: %s %s", authUser, uriUser, resource.getRequest().getMethod()
  // .getName(), resource.getRequest().getResourceRef().toString());
  // }

  /**
   * The error message for requests that generate an unspecified internal error.
   * 
   * @param resource The resource associated with this request.
   * @param logger The logger.
   * @param e The exception.
   * @return A string describing the problem.
   */
  static String internalError(WattDepotResource resource, Logger logger, Exception e) {
    String message =
        String.format("Internal error %s:%n  Request: %s %s", e.getMessage(), resource.getRequest()
            .getMethod().getName(), resource.getRequest().getResourceRef().toString());
    logger.info(String.format("%s\n%s", message, StackTrace.toString(e)));
    return message;
  }

  /**
   * The error message for requests that generate an unspecified internal error.
   * 
   * @param resource The resource associated with this request.
   * @param logger The logger.
   * @param inMessage The error message.
   * @return A string describing the problem.
   */
  static String internalError(WattDepotResource resource, Logger logger, String inMessage) {
    String message =
        String.format("Internal error %s:%n  Request: %s %s", inMessage, resource.getRequest()
            .getMethod().getName(), resource.getRequest().getResourceRef().toString());
    logger.info(message);
    return message;
  }

  /**
   * The error message for requests where a timestamp is not supplied or is not parseable.
   * 
   * @param resource The resource associated with this request.
   * @param timestamp The bogus timestamp.
   * @return A string describing the problem.
   */
  static String badTimestamp(WattDepotResource resource, String timestamp) {
    return String.format("Bad timestamp %s:%n  Request: %s %s", timestamp, resource.getRequest()
        .getMethod().getName(), resource.getRequest().getResourceRef().toString());
  }

  /**
   * The error message for requests where a sampling interval is supplied but is not parseable.
   * 
   * @param resource The resource associated with this request.
   * @param interval The bogus interval.
   * @return A string describing the problem.
   */
  static String badSamplingInterval(WattDepotResource resource, String interval) {
    return String.format("Bad sampling interval %s:%n  Request: %s %s", interval, resource
        .getRequest().getMethod().getName(), resource.getRequest().getResourceRef().toString());
  }

  /**
   * The error message for requests where an interval is specified with a start time that is greater
   * than the end time.
   * 
   * @param resource The resource associated with this request.
   * @param startTime The start time.
   * @param endTime The end time.
   * @return A string describing the problem.
   */
  static String badInterval(WattDepotResource resource, String startTime, String endTime) {
    return String.format("Bad interval, startTime %s > endTime %s:%n  Request: %s %s", startTime,
        endTime, resource.getRequest().getMethod().getName(), resource.getRequest()
            .getResourceRef().toString());
  }

  /**
   * The error message for requests where an interval is specified with a start time that is greater
   * than the end time.
   * 
   * @param resource The resource associated with this request.
   * @param startTime The start time.
   * @param endTime The end time.
   * @return A string describing the problem.
   */
  static String badRange(WattDepotResource resource, String startTime, String endTime) {
    return String.format(
        "Range extends beyond sensor data, startTime %s, endTime %s:%n  Request: %s %s", startTime,
        endTime, resource.getRequest().getMethod().getName(), resource.getRequest()
            .getResourceRef().toString());
  }

  /**
   * The error message for requests for SensorData from a Source that has no SensorData.
   * 
   * @param resource The resource associated with this request.
   * @param source The source.
   * @return A string describing the problem.
   */
  static String sourceLacksSensorData(WattDepotResource resource, String source) {
    return String.format("Source %s has no SensorData%n  Request: %s %s", source, resource
        .getRequest().getMethod().getName(), resource.getRequest().getResourceRef().toString());
  }

  /**
   * The error message for requests with a timestamp that does not exist in database.
   * 
   * @param resource The resource associated with this request.
   * @param source The source.
   * @param timestamp The timestamp.
   * @return A string describing the problem.
   */
  static String timestampNotFound(WattDepotResource resource, String source, String timestamp) {
    return String.format("Requested timestamp %s for source %s not found%n  Request: %s %s",
        timestamp, source, resource.getRequest().getMethod().getName(), resource.getRequest()
            .getResourceRef().toString());
  }

  /**
   * The error message for requests that attempt to overwrite a resource already in the database.
   * 
   * @param resource The resource associated with this request.
   * @param resourceInstance The precise instance of resource trying to be overwritten.
   * @return A string describing the problem.
   */
  static String resourceOverwrite(WattDepotResource resource, String resourceInstance) {
    return String.format("Attempted to overwrite existing resource %s%n  Request: %s %s",
        resourceInstance, resource.getRequest().getMethod().getName(), resource.getRequest()
            .getResourceRef().toString());
  }

  /**
   * The error message for unknown sources.
   * 
   * @param resource The resource associated with this request.
   * @param source The unknown source name.
   * @return A string describing the problem.
   */
  static String unknownSource(WattDepotResource resource, String source) {
    return String.format("Unknown source %s:%n  Request: %s %s", source, resource.getRequest()
        .getMethod().getName(), resource.getRequest().getResourceRef().toString());
  }

  /**
   * The error message for requests where the requesting user is not the owner.
   * 
   * @param resource The resource associated with this request.
   * @param user The user.
   * @param source The source.
   * @return A string describing the problem.
   */
  static String notSourceOwner(WattDepotResource resource, String user, String source) {
    return String.format("Authorized user %s is not owner of Source %s%n  Request: %s %s", user,
        source, resource.getRequest().getMethod().getName(), resource.getRequest().getResourceRef()
            .toString());
  }

  /**
   * The error message for requests where the credentials in a request are invalid.
   * 
   * @param resource The resource associated with this request.
   * @return A string describing the problem.
   */
  static String badCredentials(WattDepotResource resource) {
    return String.format("Invalid username and/or password provided%n  Request: %s %s", resource
        .getRequest().getMethod().getName(), resource.getRequest().getResourceRef().toString());
  }

  /**
   * The error message for miscellaneous "one off" error messages.
   * 
   * @param resource The resource associated with this request.
   * @param message A short string describing the problem.
   * @return A string describing the problem.
   */
  static String miscError(WattDepotResource resource, String message) {
    return String.format("Request generated error: %s:%n  Request: %s %s", message, resource
        .getRequest().getMethod().getName(), resource.getRequest().getResourceRef().toString());
  }

  // /**
  // * The error message for unknown users.
  // *
  // * @param resource The resource associated with this request.
  // * @param user A short string describing the problem.
  // * @return A string describing the problem.
  // */
  // static String undefinedUser(WattDepotResource resource, String user) {
  // return String.format("Undefined user %s:%n  Request: %s %s", user, resource.getRequest()
  // .getMethod().getName(), resource.getRequest().getResourceRef().toString());
  // }
  //
  // /**
  // * The error message for requests involving projects not owned by the specified user.
  // *
  // * @param resource The resource associated with this request.
  // * @param user The user.
  // * @param project The project.
  // * @return A string describing the problem.
  // */
  // static String undefinedProject(WattDepotResource resource, User user, String project) {
  // return String.format("Undefined project %s for user %s:%n  Request: %s %s", project, user
  // .getEmail(), resource.getRequest().getMethod().getName(), resource.getRequest()
  // .getResourceRef().toString());
  // }
  //
  // /**
  // * The error message for requests involving projects not owned by the specified user.
  // *
  // * @param resource The resource associated with this request.
  // * @param user The user.
  // * @param project The project.
  // * @return A string describing the problem.
  // */
  // static String cannotViewProject(WattDepotResource resource, String user, String project) {
  // return String.format("User %s not allowed to view project %s:%n  Request: %s %s", user,
  // project, resource.getRequest().getMethod().getName(), resource.getRequest()
  // .getResourceRef().toString());
  // }
  //
  // /**
  // * The error message for requests where the requesting user is not the owner.
  // *
  // * @param resource The resource associated with this request.
  // * @param user The user.
  // * @param project The project
  // * @return A string describing the problem.
  // */
  // static String notProjectOwner(WattDepotResource resource, String user, String project) {
  // return String.format("Authorized user %s is not owner of project %s%n  Request: %s %s", user,
  // project, resource.getRequest().getMethod().getName(), resource.getRequest()
  // .getResourceRef().toString());
  // }
  //

}
