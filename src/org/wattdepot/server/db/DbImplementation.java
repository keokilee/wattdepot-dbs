package org.wattdepot.server.db;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.resource.carbon.Carbon;
import org.wattdepot.resource.energy.Energy;
import org.wattdepot.resource.energy.EnergyCounterException;
import org.wattdepot.resource.property.jaxb.Properties;
import org.wattdepot.resource.property.jaxb.Property;
import org.wattdepot.resource.sensordata.SensorDataStraddle;
import org.wattdepot.resource.sensordata.StraddleList;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.sensordata.jaxb.SensorDataIndex;
import org.wattdepot.resource.sensordata.jaxb.SensorDatas;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.resource.source.jaxb.SourceIndex;
import org.wattdepot.resource.source.jaxb.Sources;
import org.wattdepot.resource.source.summary.jaxb.SourceSummary;
import org.wattdepot.resource.user.jaxb.User;
import org.wattdepot.resource.user.jaxb.UserIndex;
import org.wattdepot.server.Server;
import org.wattdepot.util.UriUtils;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * Provides a specification of the operations that must be implemented by every WattDepot server
 * storage system. Portions of this code are adapted from
 * http://hackystat-sensorbase-uh.googlecode.com/
 * 
 * @author Philip Johnson
 * @author Robert Brewer
 */

public abstract class DbImplementation {

  /** Keeps a pointer to this Server for use in accessing the managers. */
  protected Server server;

  /** Keep a pointer to the Logger. */
  protected Logger logger;

  /**
   * Constructs a new DbImplementation.
   * 
   * @param server The server.
   */
  public DbImplementation(Server server) {
    this.server = server;
    this.logger = server.getLogger();
  }

  /**
   * To be called as part of the startup process for a storage system. This method should:
   * <ul>
   * <li>Check to see if this storage system has already been created during a previous session.
   * <li>If no storage system exists, it should create one and initialize it appropriately.
   * </ul>
   * 
   * @param wipe If true, all stored data in the system should be discarded and reinitialized.
   */
  public abstract void initialize(boolean wipe);

  /**
   * Returns true if the initialize() method did indeed create a fresh storage system.
   * 
   * @return True if the storage system is freshly created.
   */
  public abstract boolean isFreshlyCreated();

  /**
   * Wipes all data from the underlying storage implementation. Obviously, this is serious
   * operation. Exposed so that tests can ensure that the database is clean before testing.
   * 
   * @return True if data could be wiped, or false if there was a problem wiping data.
   */
  public abstract boolean wipeData();

  // Start of methods based on REST API

  /**
   * Returns a list of all Sources in the system. An empty index will be returned if there are no
   * Sources in the system. The list is sorted by source name.
   * 
   * @return a SourceIndex object containing a List of SourceRefs to all Source objects.
   */
  public abstract SourceIndex getSourceIndex();

  /**
   * Returns a list of all Sources in the system as a Sources element. An empty Sources element will
   * be returned if there are no Sources in the system. The list is sorted by source name.
   * 
   * @return a Sources object containing Source objects.
   */
  public abstract Sources getSources();

  /**
   * Returns the named Source instance, or null if not found.
   * 
   * @param sourceName The name of the Source.
   * @return The requested Source, or null.
   */
  public abstract Source getSource(String sourceName);

  /**
   * Returns a SourceSummary for the named Source instance, or null if not found.
   * 
   * @param sourceName The name of the Source.
   * @return The requested SourceSummary, or null.
   */
  public abstract SourceSummary getSourceSummary(String sourceName);

  /**
   * Persists a Source instance. If a Source with this name already exists in the storage system, no
   * action is performed and the method returns false. If you wish to overwrite the resource, see
   * the two argument version of this method.
   * 
   * @param source The Source to store.
   * @return True if the user was successfully stored.
   */
  public boolean storeSource(Source source) {
    return storeSource(source, false);
  }

  /**
   * Persists a Source instance. If a Source with this name already exists in the storage system, no
   * action is performed and the method returns false, unless the overwrite parameter is true, in
   * which case the existing resource is overwritten.
   * 
   * @param source The Source to store.
   * @param overwrite False in the normal case, set to true if you wish to overwrite the resource.
   * @return True if the user was successfully stored.
   */
  public abstract boolean storeSource(Source source, boolean overwrite);

  /**
   * Ensures that the Source with the given name is no longer present in storage. All sensor data
   * associated with this Source will also be deleted.
   * 
   * @param sourceName The name of the Source.
   * @return True if the Source was deleted, or false if it was not deleted or the requested Source
   * does not exist.
   */
  public abstract boolean deleteSource(String sourceName);

  /**
   * Returns the SensorDataIndex listing all sensor data for the named Source. If the Source has no
   * SensorData resources, the index will be empty (not null). The list will be sorted in order of
   * increasing timestamp values.
   * 
   * @param sourceName The name of the Source whose sensor data is to be returned.
   * @return a SensorDataIndex object containing all relevant sensor data resources, or null if
   * sourceName is invalid.
   */
  public abstract SensorDataIndex getSensorDataIndex(String sourceName);

  /**
   * Returns the SensorDataIndex representing all the SensorData resources for the named Source such
   * that their timestamp is greater than or equal to the given start time and less than or equal to
   * the given end time. If the Source has no appropriate SensorData resources, the index will be
   * empty (not null). The list will be sorted in order of increasing timestamp values.
   * 
   * @param sourceName The name of the Source whose sensor data is to be returned.
   * @param startTime The earliest Sensor Data to be returned.
   * @param endTime The latest SensorData to be returned.
   * @throws DbBadIntervalException if startTime is later than endTime.
   * @return a SensorDataIndex object containing all relevant sensor data resources, or null if
   * sourceName, startTime, or endTime are invalid.
   */
  public abstract SensorDataIndex getSensorDataIndex(String sourceName,
      XMLGregorianCalendar startTime, XMLGregorianCalendar endTime) throws DbBadIntervalException;

  /**
   * Returns a list of all SensorData resources representing all the SensorData resources for the
   * named Source such that their timestamp is greater than or equal to the given start time and
   * less than or equal to the given end time. If the Source has no appropriate SensorData
   * resources, the SensorDatas will be empty (not null). The list will be sorted in order of
   * increasing timestamp values.
   * 
   * @param sourceName The name of the Source whose sensor data is to be returned.
   * @param startTime The earliest Sensor Data to be returned.
   * @param endTime The latest SensorData to be returned.
   * @throws DbBadIntervalException if startTime is later than endTime.
   * @return a SensorDatas object containing all relevant sensor data resources, or null if
   * sourceName, startTime, or endTime are invalid.
   */
  public abstract SensorDatas getSensorDatas(String sourceName, XMLGregorianCalendar startTime,
      XMLGregorianCalendar endTime) throws DbBadIntervalException;

  /**
   * Returns the SensorData instance for a particular named Source and timestamp, or null if not
   * found.
   * 
   * @param sourceName The name of the Source whose sensor data is to be returned.
   * @param timestamp The timestamp associated with this sensor data.
   * @return The SensorData resource, or null.
   */
  public abstract SensorData getSensorData(String sourceName, XMLGregorianCalendar timestamp);

  /**
   * Returns the latest SensorData instance for a the named non-virtual Source, or null if not
   * found. If a virtual source name is provided, null is returned.
   * 
   * @param sourceName The name of the Source whose sensor data is to be returned.
   * @return The SensorData resource, or null.
   */
  protected abstract SensorData getLatestNonVirtualSensorData(String sourceName);

  /**
   * Returns true if the passed [Source name, timestamp] has sensor data defined for it.
   * 
   * @param sourceName The name of the Source whose sensor data is to be returned.
   * @param timestamp The timestamp
   * @return True if there is any sensor data for this timestamp.
   */
  public abstract boolean hasSensorData(String sourceName, XMLGregorianCalendar timestamp);

  /**
   * Persists a SensorData instance. If SensorData with this [Source, timestamp] already exists in
   * the storage system, no action is performed and the method returns false.
   * 
   * @param data The sensor data.
   * @return True if the sensor data was successfully stored.
   */
  public abstract boolean storeSensorData(SensorData data);

  /**
   * Ensures that sensor data with the named Source and timestamp is no longer present in this
   * manager.
   * 
   * @param sourceName The name of the Source whose sensor data is to be deleted.
   * @param timestamp The timestamp associated with this sensor data.
   * @return True if the sensor data was deleted, or false if it was not deleted or the requested
   * sensor data or Source does not exist.
   */
  public abstract boolean deleteSensorData(String sourceName, XMLGregorianCalendar timestamp);

  /**
   * Ensures that all sensor data from the named Source is no longer present in storage.
   * 
   * @param sourceName The name of the Source whose sensor data is to be deleted.
   * @return True if all the sensor data was deleted, or false if it was not deleted or the
   * requested Source does not exist.
   */
  public abstract boolean deleteSensorData(String sourceName);

  /**
   * Returns a UserIndex of all Users in the system. The list is sorted by username.
   * 
   * @return a UserIndex object containing a List of UserRef objects for all User resources.
   */
  public abstract UserIndex getUsers();

  /**
   * Returns the User instance for a particular username, or null if not found.
   * 
   * @param username The user's username.
   * @return The requested User object, or null.
   */
  public abstract User getUser(String username);

  /**
   * Persists a User instance. If a User with this name already exists in the storage system, no
   * action is performed and the method returns false.
   * 
   * @param user The user to be stored.
   * @return True if the user was successfully stored.
   */
  public abstract boolean storeUser(User user);

  /**
   * Ensures that the User with the given username is no longer present in storage. All Sources
   * owned by the given User and their associated Sensor Data will be deleted as well.
   * 
   * @param username The user's username.
   * @return True if the User was deleted, or false if it was not deleted or the requested User does
   * not exist.
   */
  public abstract boolean deleteUser(String username);

  // End of methods based on REST API

  /**
   * Returns a SensorDataStraddle that straddles the given timestamp, using SensorData from the
   * given source. Note that a virtual source contains no SensorData directly, so this method will
   * always return null if the given sourceName is a virtual source. To obtain a list of
   * SensorDataStraddles for all the non-virtual subsources of a virtual source, see
   * getSensorDataStraddleList.
   * 
   * If the given timestamp corresponds to an actual SensorData, then return a degenerate
   * SensorDataStraddle with both ends of the straddle set to the actual SensorData.
   * 
   * @param sourceName The name of the source to generate the straddle from.
   * @param timestamp The timestamp of interest in the straddle.
   * @return A SensorDataStraddle that straddles the given timestamp. Returns null if: parameters
   * are null, the source doesn't exist, source has no sensor data, or there is no sensor data that
   * straddles the timestamp.
   * @see org.wattdepot.server.db.memory#getSensorDataStraddleList
   */
  public abstract SensorDataStraddle getSensorDataStraddle(String sourceName,
      XMLGregorianCalendar timestamp);

  /**
   * Returns a list of SensorDataStraddles that straddle the given timestamp, using SensorData from
   * all non-virtual subsources of the given source. If the given source is non-virtual, then the
   * result will be a list containing a single SensorDataStraddle, or null. In the case of a
   * non-virtual source, you might as well use getSensorDataStraddle.
   * 
   * @param sourceName The name of the source to generate the straddle from.
   * @param timestamp The timestamp of interest in the straddle.
   * @return A list of SensorDataStraddles that straddle the given timestamp. Returns null if:
   * parameters are null, the source doesn't exist, or there is no sensor data that straddles the
   * timestamp.
   * @see org.wattdepot.server.db.memory#getSensorDataStraddle
   */
  public abstract List<SensorDataStraddle> getSensorDataStraddleList(String sourceName,
      XMLGregorianCalendar timestamp);

  /**
   * Returns a list of StraddleLists each of which corresponds to the straddles from source (or
   * subsources of the source) for the given list of timestamps. If the given source is non-virtual,
   * then the result will be a list containing at a single StraddleList, or null. In the case of a
   * virtual source, the result is a list of StraddleLists, one for each non-virtual subsource
   * (determined recursively).
   * 
   * @param sourceName The name of the source to generate the straddle from.
   * @param timestampList The list of timestamps of interest in each straddle.
   * @return A list of StraddleLists. Returns null if: parameters are null, the source doesn't
   * exist, or there is no sensor data that straddles any of the timestamps.
   * @see org.wattdepot.server.db.memory#getSensorDataStraddle
   */
  public abstract List<StraddleList> getStraddleLists(String sourceName,
      List<XMLGregorianCalendar> timestampList);

  /**
   * Given a virtual source name, and a List of timestamps, returns a List (one member for each
   * non-virtual subsource) that contains Lists of SensorDataStraddles that straddle each of the
   * given timestamps. If the given source is non-virtual, then the result will be a list containing
   * a single List of SensorDataStraddles, or null.
   * 
   * @param sourceName The name of the source to generate the straddle from.
   * @param timestampList The list of timestamps of interest in each straddle.
   * @return A list of lists of SensorDataStraddles that straddle the given timestamp. Returns null
   * if: parameters are null, the source doesn't exist, or there is no sensor data that straddles
   * any of the timestamps.
   * @see org.wattdepot.server.db.memory#getSensorDataStraddle getSensorDataStraddle
   */
  public abstract List<List<SensorDataStraddle>> getSensorDataStraddleListOfLists(
      String sourceName, List<XMLGregorianCalendar> timestampList);

  /**
   * Returns the power in SensorData format for the Source name given and the given timestamp, or
   * null if no power data exists.
   * 
   * @param sourceName The source name.
   * @param timestamp The timestamp requested.
   * @return The requested power in SensorData format, or null if it cannot be found/calculated.
   */
  public SensorData getPower(String sourceName, XMLGregorianCalendar timestamp) {
    if (getSource(sourceName).isVirtual()) {
      List<SensorDataStraddle> straddleList = getSensorDataStraddleList(sourceName, timestamp);
      return SensorDataStraddle.getPowerFromList(straddleList, Source.sourceToUri(sourceName,
          this.server));
    }
    else {
      SensorDataStraddle straddle = getSensorDataStraddle(sourceName, timestamp);
      if (straddle == null) {
        return null;
      }
      else {
        return straddle.getPower();
      }
    }
  }

  /**
   * Returns the energy in SensorData format for the Source name given over the range of time
   * between startTime and endTime, or null if no energy data exists.
   * 
   * @param sourceName The source name.
   * @param startTime The start of the range requested.
   * @param endTime The end of the range requested.
   * @param interval The sampling interval requested (ignored if all sources support energy
   * counters).
   * @return The requested energy in SensorData format, or null if it cannot be found/calculated.
   */
  public SensorData getEnergy(String sourceName, XMLGregorianCalendar startTime,
      XMLGregorianCalendar endTime, int interval) {
    List<Source> nonVirtualSources = getAllNonVirtualSubSources(getSource(sourceName));
    // True only if all non-virtual subsources support energy counters
    boolean allSupportEnergyCounters = true;

    for (Source source : nonVirtualSources) {
      allSupportEnergyCounters =
          allSupportEnergyCounters && source.isPropertyTrue(Source.SUPPORTS_ENERGY_COUNTERS);
    }
    if (allSupportEnergyCounters) {
      List<Energy> energyList = new ArrayList<Energy>();
      // calculate energy using counters
      List<XMLGregorianCalendar> timestampList = new ArrayList<XMLGregorianCalendar>(2);
      timestampList.add(startTime);
      timestampList.add(endTime);
      List<StraddleList> sourceList = getStraddleLists(sourceName, timestampList);
      if ((sourceList == null) || (sourceList.isEmpty())) {
        return null;
      }
      // straddleList should contain a list (one for each non-virtual subsource) of StraddleLists
      // each of length 2
      for (StraddleList straddlePair : sourceList) {
        if (straddlePair.getStraddleList().size() == 2) {
          energyList.add(new Energy(straddlePair.getStraddleList().get(0), straddlePair
              .getStraddleList().get(1), true));
        }
        else {
          // One of the subsources did not have matching SensorData, so just abort
          return null;
        }
      }
      try {
        return Energy.getEnergyFromList(energyList, Source.sourceToUri(sourceName, this.server));
      }
      catch (EnergyCounterException e) {
        // some sort of counter problem. For now, we just bail and return an error
        // TODO add rollover support
        return null;
      }
    }
    else {
      List<List<SensorDataStraddle>> masterList =
          getSensorDataStraddleListOfLists(sourceName, Tstamp.getTimestampList(startTime, endTime,
              interval));
      if ((masterList == null) || (masterList.isEmpty())) {
        return null;
      }
      else {
        return Energy.getEnergyFromListOfLists(masterList, Source.sourceToUri(sourceName,
            this.server));
      }
    }
  }

  /**
   * Given a base Source, return a list of all non-virtual Sources that are subsources of the base
   * Source. This is done recursively, so virtual sources can point to other virtual sources.
   * 
   * @param baseSource The Source to start from.
   * @return A list of all non-virtual Sources that are subsources of the base Source.
   */
  public List<Source> getAllNonVirtualSubSources(Source baseSource) {
    List<Source> sourceList = new ArrayList<Source>();
    if (baseSource.isVirtual()) {
      List<Source> subSources = getAllSubSources(baseSource);
      for (Source subSource : subSources) {
        sourceList.addAll(getAllNonVirtualSubSources(subSource));
      }
      return sourceList;
    }
    else {
      sourceList.add(baseSource);
      return sourceList;
    }
  }

  /**
   * Given a Source, returns a List of Sources corresponding to any subsources of the given Source.
   * 
   * @param source The parent Source.
   * @return A List of Sources that are subsources of the given Source, or null if there are none.
   */
  public List<Source> getAllSubSources(Source source) {
    if (source.isSetSubSources()) {
      List<Source> sourceList = new ArrayList<Source>();
      for (String subSourceUri : source.getSubSources().getHref()) {
        Source subSource = getSource(UriUtils.getUriSuffix(subSourceUri));
        if (subSource != null) {
          sourceList.add(subSource);
        }
      }
      return sourceList;
    }
    else {
      return null;
    }
  }

  /**
   * Returns the carbon emitted in SensorData format for the Source name given over the range of
   * time between startTime and endTime, or null if no carbon data exists.
   * 
   * @param sourceName The source name.
   * @param startTime The start of the range requested.
   * @param endTime The start of the range requested.
   * @param interval The sampling interval requested.
   * @return The requested carbon in SensorData format, or null if it cannot be found/calculated.
   */
  public SensorData getCarbon(String sourceName, XMLGregorianCalendar startTime,
      XMLGregorianCalendar endTime, int interval) {
    List<StraddleList> masterList =
        getStraddleLists(sourceName, Tstamp.getTimestampList(startTime, endTime, interval));
    if ((masterList == null) || (masterList.isEmpty())) {
      return null;
    }
    else {
      // Make list of carbon intensities, one from each source
      return Carbon.getCarbonFromStraddleList(masterList, Source.sourceToUri(sourceName, server));
    }
  }

  /**
   * Returns the latest SensorData instance for a particular named Source, or null if not found. If
   * the Source is virtual, the latest SensorData returned is the union of all the properties of the
   * latest SensorData for each SubSource, and for any properties in common with numeric values, the
   * returned value will be the sum of all the values. The timestamp of the SensorData for a virtual
   * Source will be the <b>earliest</b> of the timestamps of the latest SensorData from each
   * SubSource, as this ensures that any subsequent requests for ranges of data using that timestamp
   * will succeed (since all SubSources have valid data up to that endpoint).
   * 
   * @param sourceName The name of the Source whose sensor data is to be returned.
   * @return The SensorData resource, or null.
   */
  public SensorData getLatestSensorData(String sourceName) {
    if (sourceName == null) {
      return null;
    }
    Source baseSource = getSource(sourceName);
    if (baseSource == null) {
      return null;
    }
    if (baseSource.isVirtual()) {
      // Storing combined properties as Map while summing to make life easier
      Map<String, Double> combinedMap = new LinkedHashMap<String, Double>();
      XMLGregorianCalendar combinedTimestamp = null;
      // Want to go through sensordata for base source, and all subsources recursively
      List<Source> sourceList = getAllNonVirtualSubSources(baseSource);
      for (Source subSource : sourceList) {
        String subSourceName = subSource.getName();
        SensorData data = getLatestNonVirtualSensorData(subSourceName);
        if (data != null) {
          // record this timestamp if it is the first we've seen or is most recent so far
          if ((combinedTimestamp == null)
              || (Tstamp.lessThan(data.getTimestamp(), combinedTimestamp))) {
            combinedTimestamp = data.getTimestamp();
          }
          // iterate over all properties found in data
          for (Property prop : data.getProperties().getProperty()) {
            Double combinedValue = combinedMap.get(prop.getKey());
            if (combinedValue == null) {
              // The combined property list does not have this property yet, so just add it verbatim
              combinedMap.put(prop.getKey(), Double.valueOf(prop.getValue()));
            }
            else {
              // Must add this property's value to existing sum. Assumes all sensor data properties
              // are doubles, which is questionable
              double newValue = combinedValue + Double.valueOf(prop.getValue());
              combinedMap.put(prop.getKey(), newValue);
            }
          }
        }
      }
      // Convert map to Properties
      Properties combinedProps = new Properties();
      for (Map.Entry<String, Double> entry : combinedMap.entrySet()) {
        combinedProps.getProperty().add(new Property(entry.getKey(), entry.getValue().toString()));
      }
      return new SensorData(combinedTimestamp, SensorData.SERVER_TOOL, baseSource.toUri(server),
          combinedProps);
    }
    else {
      // Non-virtual source, just return latest sensor data
      return getLatestNonVirtualSensorData(sourceName);
    }
  }

  /**
   * Some databases require periodic maintenance (ex. Derby requires an explicit compress command to
   * release disk space after a large number of rows have been deleted). This operation instructs
   * the underlying database to perform this maintenance. If a database implementation does not
   * support maintenance, then this command should do nothing but return true.
   * 
   * @return True if the maintenance succeeded or if the database does not support maintenance.
   */
  public abstract boolean performMaintenance();

  /**
   * The most appropriate set of indexes for the database has been evolving over time as we develop
   * new queries. This command sets up the appropriate set of indexes. It should be able to be
   * called repeatedly without error.
   * 
   * @return True if the index commands succeeded.
   */
  public abstract boolean indexTables();

  /**
   * Creates a snapshot of the database in the directory specified by
   * ServerProperties.DB_SNAPSHOT_KEY.
   * 
   * @return True if the snapshot succeeded.
   */
  public abstract boolean makeSnapshot();

  // /**
  // * Returns the current number of rows in the specified table.
  // *
  // * @param table The table whose rows are to be counted.
  // * @return The number of rows in the table, or -1 if the table does not exist or an error
  // occurs.
  // */
  // public abstract int getRowCount(String table);

  // /**
  // * Returns a set containing the names of all tables in this database. Used by clients to invoke
  // * getRowCount with a legal table name.
  // *
  // * @return A set of table names.
  // */
  // public abstract Set<String> getTableNames();
}
