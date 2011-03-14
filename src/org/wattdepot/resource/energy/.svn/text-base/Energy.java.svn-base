package org.wattdepot.resource.energy;

import java.util.List;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.resource.property.jaxb.Property;
import org.wattdepot.resource.sensordata.SensorDataStraddle;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * Represents the energy between two straddles. Currently it calculates the energy by finding the
 * area under the curve of the two (likely interpolated) power values from the straddles.
 * 
 * @author Robert Brewer
 */
public class Energy {

  /** Contains the starting straddle. */
  protected SensorDataStraddle startStraddle;

  /** Contains the ending straddle. */
  protected SensorDataStraddle endStraddle;

  /** Whether or not to use energy counters when computing energy used. */
  protected boolean useEnergyCounters;

  private static final double SECONDS_PER_HOUR = 60 * 60;

  /**
   * Creates a new Energy object given a start and end straddle.
   * 
   * @param startStraddle The start straddle.
   * @param endStraddle The end straddle.
   * @param useEnergyCounters True if energy counters should be used to compute the energy
   * @throws IllegalArgumentException If either straddle given is null.
   */
  public Energy(SensorDataStraddle startStraddle, SensorDataStraddle endStraddle,
      boolean useEnergyCounters) {
    if ((startStraddle == null) || (endStraddle == null)) {
      throw new IllegalArgumentException("Attempt to create Energy with null SensorDataStraddle");
    }
    this.startStraddle = startStraddle;
    this.endStraddle = endStraddle;
    this.useEnergyCounters = useEnergyCounters;
  }

  /**
   * Computes the amount of energy generated between the two straddles. If energy counters are used,
   * it just subtracts the start counter from the end counter. Note that if the counter has rolled
   * over, the energy value returned could be negative, and the caller is responsible for dealing
   * with this. If counters are not used, energy is calculated by computing the area inside the
   * polygon formed by the power values from the straddles.
   * 
   * @return The energy generated between the straddles in watt hours.
   */
  public double getEnergyGenerated() {
    if (this.useEnergyCounters) {
      return this.endStraddle.getEnergyGeneratedToDate()
          - this.startStraddle.getEnergyGeneratedToDate();
    }
    else {
      XMLGregorianCalendar startTime = this.startStraddle.getTimestamp();
      XMLGregorianCalendar endTime = this.endStraddle.getTimestamp();
      // the length of the range in seconds
      double rangeLength = Tstamp.diff(startTime, endTime) / 1000.0;
      double startPowerGenerated = 0, endPowerGenerated = 0;

      startPowerGenerated = this.startStraddle.getPowerGenerated();
      endPowerGenerated = this.endStraddle.getPowerGenerated();

      // compute the area of the polygon, in joules (watt seconds), then convert to Wh
      return (rangeLength * startPowerGenerated + 0.5 * rangeLength
          * (endPowerGenerated - startPowerGenerated))
          / SECONDS_PER_HOUR;
    }
  }

  /**
   * Computes the amount of energy consumed between the two straddles. If energy counters are used,
   * it just subtracts the start counter from the end counter. Note that if the counter has rolled
   * over, the energy value returned could be negative, and the caller is responsible for dealing
   * with this. If counters are not used, energy is calculated by computing the area inside the
   * polygon formed by the power values from the straddles.
   * 
   * @return The energy consumed between the straddles in watt hours.
   */
  public double getEnergyConsumed() {
    if (this.useEnergyCounters) {
      return this.endStraddle.getEnergyConsumedToDate()
          - this.startStraddle.getEnergyConsumedToDate();
    }
    else {
      XMLGregorianCalendar startTime = this.startStraddle.getTimestamp();
      XMLGregorianCalendar endTime = this.endStraddle.getTimestamp();
      // the length of the range in seconds
      double rangeLength = Tstamp.diff(startTime, endTime) / 1000.0;
      double startPowerConsumed = 0, endPowerConsumed = 0;
      startPowerConsumed = this.startStraddle.getPowerConsumed();
      endPowerConsumed = this.endStraddle.getPowerConsumed();

      // compute the area of the polygon, in joules (watt seconds), then convert to Wh
      return (rangeLength * startPowerConsumed + 0.5 * rangeLength
          * (endPowerConsumed - startPowerConsumed))
          / SECONDS_PER_HOUR;
    }
  }

  /**
   * Returns a SensorData object representing the energy between the straddles.
   * 
   * @return The SensorData object representing energy.
   */
  public SensorData getEnergy() {
    double energyGeneratedValue = getEnergyGenerated();
    double energyConsumedValue = getEnergyConsumed();

    return makeEnergySensorData(this.startStraddle.getTimestamp(), this.startStraddle
        .getBeforeData().getSource(), energyGeneratedValue, energyConsumedValue, true);
  }

  /**
   * Returns the canonical timestamp associated with this Energy object. Note that since energy is
   * defined using a pair of timestamps, the start timestamp is (arbitrarily) the canonical one.
   * 
   * @return the timestamp for this Energy object.
   */
  public XMLGregorianCalendar getTimestamp() {
    return this.startStraddle.getTimestamp();
  }

  /**
   * Creates an SensorData object that contains the given energyGenerated and energyConsumed values.
   * 
   * @param timestamp The timestamp of the SensorData to be created.
   * @param source The source URI of the SensorData to be created.
   * @param energyGeneratedValue The amount of energy generated.
   * @param energyConsumedValue The amount of energy consumed.
   * @param interpolated True if the values were determined by interpolation, false otherwise.
   * @return The new SensorData object.
   */
  public static SensorData makeEnergySensorData(XMLGregorianCalendar timestamp, String source,
      double energyGeneratedValue, double energyConsumedValue, boolean interpolated) {
    Property generatedProp, consumedProp;
    SensorData data = new SensorData(timestamp, SensorData.SERVER_TOOL, source);
    generatedProp =
        new Property(SensorData.ENERGY_GENERATED, Double.toString(energyGeneratedValue));
    data.addProperty(generatedProp);
    consumedProp = new Property(SensorData.ENERGY_CONSUMED, Double.toString(energyConsumedValue));
    data.addProperty(consumedProp);
    if (interpolated) {
      data.setInterpolated(true);
    }
    return data;
  }

  /**
   * Takes a List of SensorDataStraddles, computes and sums up the energy consumed and energy
   * generated between each straddle, and returns a new SensorData object with those sums.
   * 
   * @param straddleList The list of straddles to process.
   * @param source The URI of the Source (needed to create the SensorData).
   * @return The newly created SensorData object.
   */
  public static SensorData getEnergyFromStraddleList(List<SensorDataStraddle> straddleList,
      String source) {
    if (straddleList == null) {
      return null;
    }
    double energyGenerated = 0, energyConsumed = 0;
    boolean wasInterpolated = true;
    Energy energy;
    XMLGregorianCalendar timestamp;
    if (straddleList.isEmpty()) {
      return null;
    }
    else {
      timestamp = straddleList.get(0).getTimestamp();
      // iterate over list of straddles (note that i never reaches the max index)
      for (int i = 0; i < (straddleList.size() - 1); i++) {
        // making Energy objects of each pair of straddle. useEnergyCounters is false because
        // getEnergyFromList is only called when energy counters aren't available.
        energy = new Energy(straddleList.get(i), straddleList.get(i + 1), false);
        energyGenerated += energy.getEnergyGenerated();
        energyConsumed += energy.getEnergyConsumed();
      }
      return makeEnergySensorData(timestamp, source, energyGenerated, energyConsumed,
          wasInterpolated);
    }
  }

  /**
   * Takes a List of Energy objects, computes and sums up the energy consumed and energy generated
   * from each, and returns a new SensorData object with those sums.
   * 
   * @param energyList The list of Energy objects to process.
   * @param source The URI of the Source (needed to create the SensorData).
   * @return The newly created SensorData object.
   * @throws EnergyCounterException If using energy counters and counter in start value was smaller
   * than end value. This indicates a problem that must be handled higher up.
   */
  public static SensorData getEnergyFromList(List<Energy> energyList, String source)
      throws EnergyCounterException {
    if (energyList == null) {
      return null;
    }
    double totalEnergyGenerated = 0, totalEnergyConsumed = 0;
    double energyGenerated, energyConsumed;
    boolean wasInterpolated = true;
    XMLGregorianCalendar timestamp;
    if (energyList.isEmpty()) {
      return null;
    }
    else {
      timestamp = energyList.get(0).getTimestamp();
      // iterate over list of Energy objects
      for (Energy energy : energyList) {
        energyGenerated = energy.getEnergyGenerated();
        energyConsumed = energy.getEnergyConsumed();
        if (energyGenerated < 0) {
          throw new EnergyCounterException("computed energyGenerated was < 0: " + energyGenerated);
        }
        else if (energyConsumed < 0) {
          throw new EnergyCounterException("computed energyConsumed was < 0: " + energyConsumed);
        }
        else {
          totalEnergyGenerated += energy.getEnergyGenerated();
          totalEnergyConsumed += energy.getEnergyConsumed();
        }
      }
      return makeEnergySensorData(timestamp, source, totalEnergyGenerated, totalEnergyConsumed,
          wasInterpolated);
    }
  }

  /**
   * Takes a List of Lists of SensorDataStraddles, computes and sums up the energy consumed and
   * energy generated for each list of straddles, and returns a new SensorData object with those
   * sums.
   * 
   * @param masterList The list of lists of straddles to process.
   * @param source The URI of the Source (needed to create the SensorData).
   * @return The newly created SensorData object.
   */
  public static SensorData getEnergyFromListOfLists(List<List<SensorDataStraddle>> masterList,
      String source) {
    if (masterList == null) {
      return null;
    }
    double energyGenerated = 0, energyConsumed = 0;
    boolean wasInterpolated = true;
    SensorData energyData;
    XMLGregorianCalendar timestamp;
    if (masterList.isEmpty()) {
      return null;
    }
    else {
      timestamp = masterList.get(0).get(0).getTimestamp();
      // iterate over the list of straddle lists (each one corresponding to a different source)
      for (List<SensorDataStraddle> straddleList : masterList) {
        energyData = getEnergyFromStraddleList(straddleList, source);
        energyGenerated +=
            energyData.getProperties().getPropertyAsDouble(SensorData.ENERGY_GENERATED);
        energyConsumed +=
            energyData.getProperties().getPropertyAsDouble(SensorData.ENERGY_CONSUMED);
      }
      return makeEnergySensorData(timestamp, source, energyGenerated, energyConsumed,
          wasInterpolated);
    }
  }
}
