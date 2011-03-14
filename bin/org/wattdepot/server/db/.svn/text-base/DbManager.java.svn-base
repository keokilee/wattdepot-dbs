package org.wattdepot.server.db;

import static org.wattdepot.server.ServerProperties.DB_IMPL_KEY;
import java.lang.reflect.Constructor;
import java.util.List;
import javax.xml.datatype.XMLGregorianCalendar;
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
import org.wattdepot.server.ServerProperties;
import org.wattdepot.util.StackTrace;

/**
 * Provides an interface to storage for the resources managed by the WattDepot server. Portions of
 * this code are adapted from http://hackystat-sensorbase-uh.googlecode.com/
 * 
 * @author Philip Johnson
 * @author Robert Brewer
 */
public class DbManager {

  /** The chosen Storage system. */
  private DbImplementation dbImpl;

  /** The server using this DbManager. */
  protected Server server;

  /**
   * Creates a new DbManager which manages access to the underlying persistency layer(s). Choice of
   * which implementation of persistency layer to use is based on the ServerProperties of the server
   * provided. Instantiates the underlying storage system for use.
   * 
   * @param server The Restlet server instance.
   */
  public DbManager(Server server) {
    this(server, server.getServerProperties().get(DB_IMPL_KEY), false);
  }

  /**
   * Creates a new DbManager which manages access to the underlying persistency layer(s). Choice of
   * which implementation of persistency layer to use is based on the ServerProperties of the server
   * provided. Instantiates the underlying storage system for use.
   * 
   * @param server The Restlet server instance.
   * @param wipe If true, all stored data in the system should be discarded and reinitialized.
   */
  public DbManager(Server server, boolean wipe) {
    this(server, server.getServerProperties().get(DB_IMPL_KEY), wipe);
  }

  /**
   * Creates a new DbManager which manages access to the underlying persistency layer(s), using the
   * class name provided. This is useful for forcing a particular implementation, such as in
   * testing. Instantiates the underlying storage system for use.
   * 
   * @param server The Restlet server instance.
   * @param dbClassName The name of the DbImplementation class to instantiate.
   * @param wipe If true, all stored data in the system should be discarded and reinitialized.
   */
  public DbManager(Server server, String dbClassName, boolean wipe) {
    this.server = server;
    Class<?> dbClass = null;
    // First, try to find the class specified in the properties file (or the default)
    try {
      dbClass = Class.forName(dbClassName);
    }
    catch (ClassNotFoundException e) {
      String msg = "DB error instantiating " + dbClassName + ". Could not find this class.";
      server.getLogger().warning(msg + "\n" + StackTrace.toString(e));
      throw new IllegalArgumentException(e);
    }
    // Next, try to find a constructor that accepts a Server as its parameter.
    Class<?>[] constructorParam = { org.wattdepot.server.Server.class };
    Constructor<?> dbConstructor = null;
    try {
      dbConstructor = dbClass.getConstructor(constructorParam);
    }
    catch (Exception e) {
      String msg = "DB error instantiating " + dbClassName + ". Could not find Constructor(server)";
      server.getLogger().warning(msg + "\n" + StackTrace.toString(e));
      throw new IllegalArgumentException(e);
    }
    // Next, try to create an instance of DbImplementation from the Constructor.
    Object[] serverArg = { server };
    try {
      this.dbImpl = (DbImplementation) dbConstructor.newInstance(serverArg);
    }
    catch (Exception e) {
      String msg = "DB error instantiating " + dbClassName + ". Could not create instance.";
      server.getLogger().warning(msg + "\n" + StackTrace.toString(e));
      throw new IllegalArgumentException(e);
    }
    this.dbImpl.initialize(wipe);
    ServerProperties serverProps =
        (ServerProperties) server.getContext().getAttributes().get("ServerProperties");
    String adminUsername = serverProps.get(ServerProperties.ADMIN_EMAIL_KEY);
    String adminPassword = serverProps.get(ServerProperties.ADMIN_PASSWORD_KEY);
    // Ensure that we have an admin user
    if (this.dbImpl.getUser(adminUsername) == null) {
      // create the admin User object based on the server properties
      User adminUser = new User(adminUsername, adminPassword, true, null);
      // stick admin user into database
      if (!this.dbImpl.storeUser(adminUser)) {
        server.getLogger().severe("Unable to create admin user on DbManager creation!");
      }
    }
  }

  /**
   * Returns a list of all Sources in the system. An empty index will be returned if there are no
   * Sources in the system. The list is sorted by source name.
   * 
   * @return a SourceIndex object containing a List of SourceRefs to all Source objects.
   */
  public SourceIndex getSourceIndex() {
    return this.dbImpl.getSourceIndex();
  }

  /**
   * Returns a list of all Sources in the system as a Sources element. An empty Sources element will
   * be returned if there are no Sources in the system. The list is sorted by source name.
   * 
   * @return a Sources object containing Source objects.
   */
  public Sources getSources() {
    return this.dbImpl.getSources();
  }

  /**
   * Returns the named Source instance, or null if not found.
   * 
   * @param sourceName The name of the Source.
   * @return The requested Source, or null.
   */
  public Source getSource(String sourceName) {
    return this.dbImpl.getSource(sourceName);
  }

  /**
   * Returns a SourceSummary for the named Source instance, or null if not found.
   * 
   * @param sourceName The name of the Source.
   * @return The requested SourceSummary, or null.
   */
  public SourceSummary getSourceSummary(String sourceName) {
    return this.dbImpl.getSourceSummary(sourceName);
  }

  /**
   * Persists a Source instance. If a Source with this name already exists in the storage system, no
   * action is performed and the method returns false. If you wish to overwrite the resource, see
   * the two argument version of this method.
   * 
   * @param source The Source to store.
   * @return True if the user was successfully stored.
   */
  public boolean storeSource(Source source) {
    return this.dbImpl.storeSource(source);
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
  public boolean storeSource(Source source, boolean overwrite) {
    return this.dbImpl.storeSource(source, overwrite);
  }

  /**
   * Ensures that the Source with the given name is no longer present in storage. All sensor data
   * associated with this Source will also be deleted.
   * 
   * @param sourceName The name of the Source.
   * @return True if the Source was deleted, or false if it was not deleted or the requested Source
   * does not exist.
   */
  public boolean deleteSource(String sourceName) {
    return this.dbImpl.deleteSource(sourceName);
  }

  /**
   * Returns the SensorDataIndex listing all sensor data for the named Source. If the Source has no
   * SensorData resources, the index will be empty (not null). The list will be sorted in order of
   * increasing timestamp values.
   * 
   * @param sourceName The name of the Source whose sensor data is to be returned.
   * @return a SensorDataIndex object containing all relevant sensor data resources, or null if
   * sourceName is invalid.
   */
  public SensorDataIndex getSensorDataIndex(String sourceName) {
    return this.dbImpl.getSensorDataIndex(sourceName);
  }

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
  public SensorDataIndex getSensorDataIndex(String sourceName, XMLGregorianCalendar startTime,
      XMLGregorianCalendar endTime) throws DbBadIntervalException {
    return this.dbImpl.getSensorDataIndex(sourceName, startTime, endTime);
  }

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
  public SensorDatas getSensorDatas(String sourceName, XMLGregorianCalendar startTime,
      XMLGregorianCalendar endTime) throws DbBadIntervalException {
    return this.dbImpl.getSensorDatas(sourceName, startTime, endTime);
  }

  /**
   * Returns the SensorData instance for a particular named Source and timestamp, or null if not
   * found.
   * 
   * @param sourceName The name of the Source whose sensor data is to be returned.
   * @param timestamp The timestamp associated with this sensor data.
   * @return The SensorData resource, or null.
   */
  public SensorData getSensorData(String sourceName, XMLGregorianCalendar timestamp) {
    return this.dbImpl.getSensorData(sourceName, timestamp);
  }

  /**
   * Returns the latest SensorData instance for a particular named Source, or null if not found.
   * 
   * @param sourceName The name of the Source whose sensor data is to be returned.
   * @return The SensorData resource, or null.
   */
  public SensorData getLatestSensorData(String sourceName) {
    return this.dbImpl.getLatestSensorData(sourceName);
  }

  /**
   * Returns true if the passed [Source name, timestamp] has sensor data defined for it.
   * 
   * @param sourceName The name of the Source whose sensor data is to be returned.
   * @param timestamp The timestamp
   * @return True if there is any sensor data for this timestamp.
   */
  public boolean hasSensorData(String sourceName, XMLGregorianCalendar timestamp) {
    return this.dbImpl.hasSensorData(sourceName, timestamp);
  }

  /**
   * Persists a SensorData instance. If SensorData with this [Source, timestamp] already exists in
   * the storage system, no action is performed and the method returns false.
   * 
   * @param data The sensor data.
   * @return True if the sensor data was successfully stored.
   */
  public boolean storeSensorData(SensorData data) {
    return this.dbImpl.storeSensorData(data);
  }

  /**
   * Ensures that sensor data with the named Source and timestamp is no longer present in this
   * manager.
   * 
   * @param sourceName The name of the Source whose sensor data is to be deleted.
   * @param timestamp The timestamp associated with this sensor data.
   * @return True if the sensor data was deleted, or false if it was not deleted or the requested
   * sensor data or Source does not exist.
   */
  public boolean deleteSensorData(String sourceName, XMLGregorianCalendar timestamp) {
    return this.dbImpl.deleteSensorData(sourceName, timestamp);
  }

  /**
   * Ensures that all sensor data from the named Source is no longer present in storage.
   * 
   * @param sourceName The name of the Source whose sensor data is to be deleted.
   * @return True if all the sensor data was deleted, or false if it was not deleted or the
   * requested Source does not exist.
   */
  public boolean deleteSensorData(String sourceName) {
    return this.dbImpl.deleteSensorData(sourceName);
  }

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
  public SensorDataStraddle getSensorDataStraddle(String sourceName, XMLGregorianCalendar timestamp) {
    return this.dbImpl.getSensorDataStraddle(sourceName, timestamp);
  }

  /**
   * Returns a list of SensorDataStraddles that straddle the given timestamp, using SensorData from
   * all non-virtual subsources of the given source. If the given source is non-virtual, then the
   * result will be a list containing at a single SensorDataStraddle, or null. In the case of a
   * non-virtual source, you might as well use getSensorDataStraddle.
   * 
   * @param sourceName The name of the source to generate the straddle from.
   * @param timestamp The timestamp of interest in the straddle.
   * @return A list of SensorDataStraddles that straddle the given timestamp. Returns null if:
   * parameters are null, the source doesn't exist, or there is no sensor data that straddles the
   * timestamp.
   * @see org.wattdepot.server.db.memory#getSensorDataStraddle
   */
  public List<SensorDataStraddle> getSensorDataStraddleList(String sourceName,
      XMLGregorianCalendar timestamp) {
    return this.dbImpl.getSensorDataStraddleList(sourceName, timestamp);
  }

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
  public List<StraddleList> getStraddleLists(String sourceName,
      List<XMLGregorianCalendar> timestampList) {
    return this.dbImpl.getStraddleLists(sourceName, timestampList);
  }

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
  public List<List<SensorDataStraddle>> getSensorDataStraddleListOfLists(String sourceName,
      List<XMLGregorianCalendar> timestampList) {
    return this.dbImpl.getSensorDataStraddleListOfLists(sourceName, timestampList);
  }

  /**
   * Returns the power in SensorData format for the Source name given in the URI and the given
   * timestamp, or null if no power data exists.
   * 
   * @param sourceName The source name.
   * @param timestamp The timestamp requested.
   * @return The requested power in SensorData format, or null if it cannot be found/calculated.
   */
  public SensorData getPower(String sourceName, XMLGregorianCalendar timestamp) {
    return this.dbImpl.getPower(sourceName, timestamp);
  }

  /**
   * Returns the energy in SensorData format for the Source name given over the range of time
   * between startTime and endTime, or null if no energy data exists.
   * 
   * @param sourceName The source name.
   * @param startTime The start of the range requested.
   * @param endTime The start of the range requested.
   * @param interval The sampling interval requested.
   * @return The requested energy in SensorData format, or null if it cannot be found/calculated.
   */
  public SensorData getEnergy(String sourceName, XMLGregorianCalendar startTime,
      XMLGregorianCalendar endTime, int interval) {
    return this.dbImpl.getEnergy(sourceName, startTime, endTime, interval);
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
    return this.dbImpl.getCarbon(sourceName, startTime, endTime, interval);
  }

  /**
   * Given a base Source, return a list of all non-virtual Sources that are subsources of the base
   * Source. This is done recursively, so virtual sources can point to other virtual sources.
   * 
   * @param baseSource The Source to start from.
   * @return A list of all non-virtual Sources that are subsources of the base Source.
   */
  public List<Source> getAllNonVirtualSubSources(Source baseSource) {
    return this.dbImpl.getAllNonVirtualSubSources(baseSource);
  }

  /**
   * Given a Source, returns a List of Sources corresponding to any subsources of the given Source.
   * 
   * @param source The parent Source.
   * @return A List of Sources that are subsources of the given Source, or null if there are none.
   */
  public List<Source> getAllSubSources(Source source) {
    return this.dbImpl.getAllSubSources(source);
  }

  /**
   * Returns a UserIndex of all Users in the system. The list is sorted by username.
   * 
   * @return a UserIndex object containing a List of UserRef objects for all User resources.
   */
  public UserIndex getUsers() {
    return this.dbImpl.getUsers();
  }

  /**
   * Returns the User instance for a particular username, or null if not found.
   * 
   * @param username The user's username.
   * @return The requested User object, or null.
   */
  public User getUser(String username) {
    return this.dbImpl.getUser(username);
  }

  /**
   * Persists a User instance. If a User with this name already exists in the storage system, no
   * action is performed and the method returns false.
   * 
   * @param user The user to be stored.
   * @return True if the user was successfully stored.
   */
  public boolean storeUser(User user) {
    return this.dbImpl.storeUser(user);
  }

  /**
   * Ensures that the User with the given username is no longer present in storage. All Sources
   * owned by the given User and their associated Sensor Data will be deleted as well.
   * 
   * @param username The user's username.
   * @return True if the User was deleted, or false if it was not deleted or the requested User does
   * not exist.
   */
  public boolean deleteUser(String username) {
    return this.dbImpl.deleteUser(username);
  }

  /**
   * Some databases require periodic maintenance (ex. Derby requires an explicit compress command to
   * release disk space after a large number of rows have been deleted). This operation instructs
   * the underlying database to perform this maintenance. If a database implementation does not
   * support maintenance, then this command should do nothing but return true.
   * 
   * @return True if the maintenance succeeded or if the database does not support maintenance.
   */
  public boolean performMaintenance() {
    return this.dbImpl.performMaintenance();
  }

  /**
   * The most appropriate set of indexes for the database has been evolving over time as we develop
   * new queries. This command sets up the appropriate set of indexes. It should be able to be
   * called repeatedly without error.
   * 
   * @return True if the index commands succeeded.
   */
  public boolean indexTables() {
    return this.dbImpl.indexTables();
  }

  /**
   * Creates a snapshot of the database in the directory specified by
   * ServerProperties.DB_SNAPSHOT_KEY.
   * 
   * @return True if the snapshot succeeded.
   */
  public boolean makeSnapshot() {
    return this.dbImpl.makeSnapshot();
  }

  /**
   * Wipes all data from the underlying storage implementation. Obviously, this is serious
   * operation. Exposed so that tests can ensure that the database is clean before testing.
   * 
   * @return True if data could be wiped, or false if there was a problem wiping data.
   */
  public boolean wipeData() {
    return this.dbImpl.wipeData();
  }
}