package org.wattdepot.server.db;

import javax.xml.datatype.XMLGregorianCalendar;

/**
 * An exception that is thrown when the database system encounters a time interval specified by two
 * timestamps, but the starting timestamp is later than the ending timestamp.
 * 
 * @author Robert Brewer
 */
public class DbBadIntervalException extends DbException {

  /**
   * The serialization UID, not that we'll ever use it.
   */
  private static final long serialVersionUID = -6477930268980330868L;

  /**
   * Thrown when an the start time of an interval is later than the end time of the interval.
   * 
   * @param startTime The bad start time.
   * @param endTime The bad end time.
   */
  public DbBadIntervalException(XMLGregorianCalendar startTime, XMLGregorianCalendar endTime) {
    super("startTime " + startTime.toString() + " is later than endTime " + endTime.toString());
  }
}
