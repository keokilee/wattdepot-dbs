package org.wattdepot.util;

/**
 * Provides utility methods for working on URIs.
 * 
 * @author Robert Brewer
 */
public class UriUtils {

  /**
   * Returns the final segment of a URI, i.e. everything after the final "/". This method is useful
   * for coverting fields that are URIs into the name of the resource, when constructing new URIs.
   * For example, SensorData resources contain the URI of their Source, but if you wish to store a
   * SensorData object you will need to construct a URI that contains the name of the Source, not
   * the whole URI to the Source.
   * 
   * @param uri The URI to be processed.
   * @return the final suffix of the URI, or null if there are no instances of "/" in the URI.
   */
  public static String getUriSuffix(String uri) {
    if (uri.lastIndexOf('/') == -1) {
      return null;
    }
    else {
      return uri.substring(uri.lastIndexOf('/') + 1);
    }
  }
}
