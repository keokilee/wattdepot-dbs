package org.wattdepot.resource.energy;

/**
 * Thrown when an energy counter in a sensor datum from a Source is lower than a previous energy
 * counter value. In normal operation, counters should be monotonically increasing so detecting
 * a decreasing value represents an exceptional situation that must be handled at a higher level.
 * 
 * The common reasons for a counter value decreasing are: a register overflow in the meter providing
 * data, or replacement of an existing meter with a new one. 
 * 
 * @author Robert Brewer
 */
public class EnergyCounterException extends Exception {

  /** The default serial version UID. */
  private static final long serialVersionUID = 1L;

  /**
   * Thrown when an energy counter in a sensor datum from a Source is lower than a previous energy
   * counter value. Since counters should be monotonically increasing, calling method must decide
   * how to handle the situation.
   */
  public EnergyCounterException() {
    super();
  }

  /**
   * Thrown when an energy counter in a sensor datum from a Source is lower than a previous energy
   * counter value. Since counters should be monotonically increasing, calling method must decide
   * how to handle the situation.
   * 
   * @param message A string containing any relevant error message.
   */
  public EnergyCounterException(String message) {
    super(message);
  }
}
