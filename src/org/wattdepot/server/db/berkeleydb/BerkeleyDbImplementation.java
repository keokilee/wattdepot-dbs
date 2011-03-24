package org.wattdepot.server.db.berkeleydb;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.JAXBException;
import javax.xml.datatype.DatatypeConstants;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.resource.sensordata.SensorDataStraddle;
import org.wattdepot.resource.sensordata.StraddleList;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.sensordata.jaxb.SensorDataIndex;
import org.wattdepot.resource.sensordata.jaxb.SensorDataRef;
import org.wattdepot.resource.sensordata.jaxb.SensorDatas;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.resource.source.jaxb.SourceIndex;
import org.wattdepot.resource.source.jaxb.Sources;
import org.wattdepot.resource.source.summary.jaxb.SourceSummary;
import org.wattdepot.resource.user.jaxb.User;
import org.wattdepot.resource.user.jaxb.UserIndex;
import org.wattdepot.resource.user.jaxb.UserRef;
import org.wattdepot.server.Server;
import org.wattdepot.server.db.DbBadIntervalException;
import org.wattdepot.server.db.DbImplementation;
import org.wattdepot.util.StackTrace;
import org.wattdepot.util.tstamp.Tstamp;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.util.DbBackup;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.sleepycat.persist.StoreConfig;

/**
 * WattDepot DbImplementation using BerkeleyDB as the data store. This is an alternative
 * implementation to be used to compare against other DbImplementations.
 * 
 * @author George Lee
 * 
 */
public class BerkeleyDbImplementation extends DbImplementation {
  private static final String UNABLE_TO_PARSE_PROPERTY_XML =
    "Unable to parse property XML from database ";
  
  private boolean isFreshlyCreated;
  private PrimaryIndex<CompositeSensorDataKey, BerkeleyDbSensorData> sensorDataIndex;
  private PrimaryIndex<String, BerkeleyDbUser> userIndex;
  private PrimaryIndex<String, BerkeleyDbSource> sourceIndex; 
  private Environment environment;
  private long lastBackupFileId;
  private File backupDir;
  
  /**
   * Instantiates the BerkeleyDB installation.
   * 
   * @param server The server this implementation is associated with.
   */
  public BerkeleyDbImplementation(Server server) {
    super(server);
  }
  
  @Override
  public void initialize(boolean wipe) {
    // Construct directories.
    String currDir = System.getProperty("user.dir");
    File topDir = new File(currDir, "berkeleyDb");
    boolean success = topDir.mkdirs();
    if (success) {
      System.out.println("Created the berkeleyDb directory.");
    }
    
    File dir = new File(topDir, "sensorDataDb");
    success = dir.mkdirs();
    if (success) {
      System.out.println("Created the sensorData directory.");
    }
    
    dir = new File(topDir, "sourceDb");
    success = dir.mkdirs();
    if (success) {
      System.out.println("Created the source directory.");
    }
    
    dir = new File(topDir, "userDb");
    success = dir.mkdirs();
    if (success) {
      System.out.println("Created the user directory.");
    }
    
    this.backupDir = new File(topDir, "backup");
    success = this.backupDir.mkdirs();
    if (success) {
      System.out.println("Created the backup directory.");
    }
    
    // If any directory was created, then this was previously uninitialized.
    this.isFreshlyCreated = success;
    String dbStatusMsg =
        (this.isFreshlyCreated) ? "BerkeleyDB: uninitialized." : "BerkeleyDB: previously initialized.";
    this.logger.info(dbStatusMsg);
    
    // Check if we have any pre-existing backups.
    if (this.isFreshlyCreated) {
      //No backups, so start from zero.
      this.lastBackupFileId = 0;
    }
    else {
      this.lastBackupFileId = this.getLastBackedUpFile();
    }
    
    // Configure BerkeleyDB.
    EnvironmentConfig envConfig = new EnvironmentConfig();
    StoreConfig storeConfig = new StoreConfig();
    envConfig.setAllowCreate(true);
    storeConfig.setAllowCreate(true);
    this.environment = new Environment(topDir, envConfig);
    
    //Initialize data stores.
    EntityStore sensorDataStore = new EntityStore(this.environment, "EntityStore", storeConfig);
    this.sensorDataIndex = sensorDataStore.getPrimaryIndex(
        CompositeSensorDataKey.class, BerkeleyDbSensorData.class);
    EntityStore userStore = new EntityStore(this.environment, "EntityStore", storeConfig);
    this.userIndex = userStore.getPrimaryIndex(
        String.class, BerkeleyDbUser.class);
    EntityStore sourceStore = new EntityStore(this.environment, "EntityStore", storeConfig);
    this.sourceIndex = sourceStore.getPrimaryIndex(
        String.class, BerkeleyDbSource.class);
    
    // Guarantee that the environment is closed upon system exit.
    List<EntityStore> stores = new ArrayList<EntityStore>();
    stores.add(sensorDataStore);
    stores.add(sourceStore);
    stores.add(userStore);
    DbShutdownHook shutdownHook = new DbShutdownHook(this.environment, stores);
    Runtime.getRuntime().addShutdownHook(shutdownHook);
    
    if (wipe) {
      this.wipeData();
    }
  }
  
  /**
   * Goes through the backup folder to find the last backed up file.
   * 
   * @return The id of the last backed up file.
   */
  private long getLastBackedUpFile() {
    // Open the backup folder
    long lastBackup = 0;
    File[] files = this.backupDir.listFiles();
    String filename, substring;
    
    // Go through the files and find files that end with .jdb.
    for (File file : files) {
      if (!file.isDirectory() && file.getName().endsWith(".jdb")) {
        // Parse filename to get the id.
        filename = file.getName();
        substring = filename.substring(0, filename.length() - 4);
        if (lastBackup < Long.parseLong(substring, 16)) {
          lastBackup = Long.parseLong(substring, 16);
        }
      }
    }
    
    return lastBackup;
  }

  @Override
  public boolean isFreshlyCreated() {
    return this.isFreshlyCreated;
  }

  @Override
  public boolean deleteSensorData(String sourceName, XMLGregorianCalendar timestamp) {
    if (sourceName == null || timestamp == null) {
      return false;
    }
    
    String sourceUri = Source.sourceToUri(sourceName, this.server.getHostName());
    CompositeSensorDataKey key = new CompositeSensorDataKey(sourceUri, timestamp);
    return sensorDataIndex.delete(key);
  }

  @Override
  public boolean deleteSensorData(String sourceName) {
    if (sourceName == null) {
      return false;
    }
    
    // Construct the range of sensor data.
    String sourceUri = Source.sourceToUri(sourceName, this.server.getHostName());
    CompositeSensorDataKey start = new CompositeSensorDataKey(sourceUri, Tstamp.makeTimestamp(0));
    CompositeSensorDataKey end = new CompositeSensorDataKey(sourceUri, Tstamp.makeTimestamp());
    EntityCursor<BerkeleyDbSensorData> cursor = sensorDataIndex.entities(start, true, end, true);
    
    int count = 0;
    while (cursor.next() != null) {
      cursor.delete();
      count++;
    }
    
    cursor.close();
    return count > 0;
  }

  @Override
  public boolean deleteSource(String sourceName) {
    if (sourceName == null) {
      return false;
    }
    
    return sourceIndex.delete(sourceName);
  }

  @Override
  public boolean deleteUser(String username) {
    if (username == null) {
      return false;
    }
    
    return userIndex.delete(username);
  }

  @Override
  protected SensorData getLatestNonVirtualSensorData(String sourceName) {
    if (sourceName == null) {
      return null;
    }
    String sourceUri = Source.sourceToUri(sourceName, this.server.getHostName());
    CompositeSensorDataKey start = new CompositeSensorDataKey(sourceUri, Tstamp.makeTimestamp(0));
    CompositeSensorDataKey end = new CompositeSensorDataKey(sourceUri, Tstamp.makeTimestamp());
    EntityCursor<BerkeleyDbSensorData> cursor = sensorDataIndex.entities(start, true, end, true);
    BerkeleyDbSensorData dbData = cursor.last();
    cursor.close();
    
    if (dbData == null) {
      this.logger.warning("Could not find any db data.");
      return null;
    }
    
    try {
      return dbData.asSensorData();
    }
    catch (JAXBException e) {
      this.logger.warning(UNABLE_TO_PARSE_PROPERTY_XML + StackTrace.toString(e));
      return null;
    }
  }

  @Override
  public SensorData getSensorData(String sourceName, XMLGregorianCalendar timestamp) {
    if (sourceName == null || timestamp == null) {
      return null;
    }
    String sourceUri = Source.sourceToUri(sourceName, this.server.getHostName());
    CompositeSensorDataKey key = new CompositeSensorDataKey(sourceUri, timestamp);
    BerkeleyDbSensorData data = sensorDataIndex.get(key);
    if (data == null) {
      return null;
    }
    
    try {
      return data.asSensorData();
    }
    catch (JAXBException e) {
      this.logger.warning(UNABLE_TO_PARSE_PROPERTY_XML + StackTrace.toString(e));
      return null;
    }
  }

  @Override
  public SensorDataIndex getSensorDataIndex(String sourceName) {
    if (sourceName == null) {
      return null;
    }
    
    try {
      return this.getSensorDataIndex(sourceName, Tstamp.makeTimestamp(0), Tstamp.makeTimestamp());
    }
    catch (DbBadIntervalException e) {
      // This shouldn't happen.
      this.logger.warning("getSensorDataIndex failed for an interval from 0 to today.");
      return null;
    }
  }

  @Override
  public SensorDataIndex getSensorDataIndex(String sourceName, XMLGregorianCalendar startTime,
      XMLGregorianCalendar endTime) throws DbBadIntervalException {
    if (sourceName == null || startTime == null || endTime == null) {
      return null;
    }
    else if (this.getSource(sourceName) == null) {
      // Unknown Source name, therefore no possibility of SensorData
      return null;
    }
    else if (startTime.compare(endTime) == DatatypeConstants.GREATER) {
      // startTime > endTime, which is bogus
      throw new DbBadIntervalException(startTime, endTime);
    }
    
    //Construct the range
    String sourceUri = Source.sourceToUri(sourceName, this.server.getHostName());
    CompositeSensorDataKey start = new CompositeSensorDataKey(sourceUri, startTime);
    CompositeSensorDataKey end = new CompositeSensorDataKey(sourceUri, endTime);
    EntityCursor<BerkeleyDbSensorData> cursor = sensorDataIndex.entities(start, true, end, true);
    
    //Iterate over the results and add refs.
    SensorDataIndex index = new SensorDataIndex();
    for (BerkeleyDbSensorData data : cursor) {
      SensorDataRef ref = new SensorDataRef(data.getTimestamp(), data.getTool(), data.getSource());
      index.getSensorDataRef().add(ref);
    }
    cursor.close();
    return index;
  }

  @Override
  public SensorDataStraddle getSensorDataStraddle(String sourceName, XMLGregorianCalendar timestamp) {
    if (sourceName == null || timestamp == null) {
      return null;
    }
    else if (this.getSource(sourceName) == null) {
      // Unknown Source name, therefore no possibility of SensorData
      return null;
    }
    
    SensorData data = getSensorData(sourceName, timestamp);
    if (data == null) {
      BerkeleyDbSensorData dbData;
      SensorData beforeData, afterData;
      
      // Grab the item immediately previous.
      String sourceUri = Source.sourceToUri(sourceName, this.server.getHostName());
      CompositeSensorDataKey start = new CompositeSensorDataKey(sourceUri, Tstamp.makeTimestamp(0));
      CompositeSensorDataKey end = new CompositeSensorDataKey(sourceUri, timestamp);
      EntityCursor<BerkeleyDbSensorData> cursor = sensorDataIndex.entities(start, true, end, true);
      dbData = cursor.last();
      cursor.close();
      if (dbData == null) {
        //No previous data, so no straddle.
        return null;
      }
      try {
        beforeData = dbData.asSensorData();
      }
      catch (JAXBException e) {
        this.logger.warning(UNABLE_TO_PARSE_PROPERTY_XML + StackTrace.toString(e));
        return null;
      }
      
      // Grab the item immediately after.
      start = new CompositeSensorDataKey(sourceUri, timestamp);
      end = new CompositeSensorDataKey(sourceUri, Tstamp.makeTimestamp());
      cursor = sensorDataIndex.entities(start, true, end, true);
      dbData = cursor.first();
      cursor.close();
      if (dbData == null) {
        //No post data, so no straddle.
        return null;
      }
      try {
        afterData = dbData.asSensorData();
      }
      catch (JAXBException e) {
        this.logger.warning(UNABLE_TO_PARSE_PROPERTY_XML + StackTrace.toString(e));
        return null;
      }
      
      return new SensorDataStraddle(timestamp, beforeData, afterData);
    }
    
    // We have data for this timestamp, so just return the same data twice.
    return new SensorDataStraddle(timestamp, data, data);
  }

  @Override
  public List<SensorDataStraddle> getSensorDataStraddleList(String sourceName,
      XMLGregorianCalendar timestamp) {
    if ((sourceName == null) || (timestamp == null)) {
      return null;
    }
    Source baseSource = getSource(sourceName);
    if (baseSource == null) {
      return null;
    }
    
    // Want to go through sensordata for base source, and all subsources recursively
    List<Source> sourceList = getAllNonVirtualSubSources(baseSource);
    List<SensorDataStraddle> straddleList = new ArrayList<SensorDataStraddle>(sourceList.size());
    for (Source subSource : sourceList) {
      String subSourceName = subSource.getName();
      SensorDataStraddle straddle = getSensorDataStraddle(subSourceName, timestamp);
      if (straddle == null) {
        // No straddle for this timestamp on this source, abort
        return null;
      }
      else {
        straddleList.add(straddle);
      }
    }
    if (straddleList.isEmpty()) {
      return null;
    }
    else {
      return straddleList;
    }
  }

  @Override
  public List<List<SensorDataStraddle>> getSensorDataStraddleListOfLists(String sourceName,
      List<XMLGregorianCalendar> timestampList) {
    List<List<SensorDataStraddle>> masterList = new ArrayList<List<SensorDataStraddle>>();
    if ((sourceName == null) || (timestampList == null)) {
      return null;
    }
    Source baseSource = getSource(sourceName);
    if (baseSource == null) {
      return null;
    }
    // Want to go through sensordata for base source, and all subsources recursively
    List<Source> sourceList = getAllNonVirtualSubSources(baseSource);
    for (Source subSource : sourceList) {
      List<SensorDataStraddle> straddleList = new ArrayList<SensorDataStraddle>();
      String subSourceName = subSource.getName();
      for (XMLGregorianCalendar timestamp : timestampList) {
        SensorDataStraddle straddle = getSensorDataStraddle(subSourceName, timestamp);
        if (straddle == null) {
          // No straddle for this timestamp on this source, abort
          return null;
        }
        else {
          straddleList.add(straddle);
        }
      }
      masterList.add(straddleList);
    }
    if (masterList.isEmpty()) {
      return null;
    }
    else {
      return masterList;
    }
  }

  @Override
  public SensorDatas getSensorDatas(String sourceName, XMLGregorianCalendar startTime,
      XMLGregorianCalendar endTime) throws DbBadIntervalException {
    if ((sourceName == null) || (startTime == null) || (endTime == null)) {
      return null;
    }
    else if (getSource(sourceName) == null) {
      // Unknown Source name, therefore no possibility of SensorData
      return null;
    }
    else if (startTime.compare(endTime) == DatatypeConstants.GREATER) {
      // startTime > endTime, which is bogus
      throw new DbBadIntervalException(startTime, endTime);
    }
    
    String sourceUri = Source.sourceToUri(sourceName, this.server.getHostName());
    CompositeSensorDataKey start = new CompositeSensorDataKey(sourceUri, startTime);
    CompositeSensorDataKey end = new CompositeSensorDataKey(sourceUri, endTime);
    EntityCursor<BerkeleyDbSensorData> cursor = sensorDataIndex.entities(start, true, end, true);
    
    SensorDatas datas = new SensorDatas();
    for (BerkeleyDbSensorData data : cursor) {
      try {
        datas.getSensorData().add(data.asSensorData());
      }
      catch (JAXBException e) {
        this.logger.warning(UNABLE_TO_PARSE_PROPERTY_XML + StackTrace.toString(e));
      }
    }
    cursor.close();
    return datas;
  }

  @Override
  public Source getSource(String sourceName) {
    if (sourceName == null) {
      return null;
    }
    
    BerkeleyDbSource dbSource = sourceIndex.get(sourceName);
    if (dbSource == null) {
      return null;
    }
    
    try {
      return dbSource.asSource();
    }
    catch (JAXBException e) {
      this.logger.warning(UNABLE_TO_PARSE_PROPERTY_XML + StackTrace.toString(e));
      return null;
    }
  }

  @Override
  public SourceIndex getSourceIndex() {
    SourceIndex index = new SourceIndex();
    EntityCursor<BerkeleyDbSource> cursor = sourceIndex.entities();
    
    for (BerkeleyDbSource source : cursor) {
      index.getSourceRef().add(source.asSourceRef(this.server));
    }
    cursor.close();
    return index;
  }

  @Override
  public SourceSummary getSourceSummary(String sourceName) {
    if (sourceName == null) {
      // null or non-existent source name
      return null;
    }
    Source baseSource = getSource(sourceName);
    if (baseSource == null) {
      return null;
    }
    SourceSummary summary = new SourceSummary();
    summary.setHref(Source.sourceToUri(sourceName, this.server.getHostName()));
    
    // Want to go through sensordata for base source, and all subsources recursively
    List<Source> sourceList = getAllNonVirtualSubSources(baseSource);
    XMLGregorianCalendar firstTimestamp = null, lastTimestamp = null;
    int dataCount = 0;
    BerkeleyDbSensorData temp;
    String subsourceUri;
    CompositeSensorDataKey start, end;
    EntityCursor<BerkeleyDbSensorData> cursor;
    for (Source subSource : sourceList) {
      subsourceUri = Source.sourceToUri(subSource.getName(), this.server.getHostName());
      //Create cursor for getting data.
      start = new CompositeSensorDataKey(subsourceUri, Tstamp.makeTimestamp(0));
      end = new CompositeSensorDataKey(subsourceUri, Tstamp.makeTimestamp());
      cursor = sensorDataIndex.entities(start, true, end, true);
      
      //Get first timestamp of sensor data.
      if ((temp = cursor.first()) != null && (firstTimestamp == null ||
          Tstamp.lessThan(temp.getTimestamp(), firstTimestamp))) {
        firstTimestamp = temp.getTimestamp();
      }
      
      //Iterate through and count up the number of items
      //Note that we are already starting from the first, so we add one extra.
      dataCount++;
      while (cursor.next() != null) {
        dataCount++;
      }
      
      //Get last timestamp of sensor data.
      if ((temp = cursor.last()) != null && (lastTimestamp == null ||
          Tstamp.greaterThan(temp.getTimestamp(), lastTimestamp))) {
        lastTimestamp = temp.getTimestamp();
      }
      
      
      //Clean up
      cursor.close();
    }
    
    summary.setFirstSensorData(firstTimestamp);
    summary.setLastSensorData(lastTimestamp);
    summary.setTotalSensorDatas(dataCount);
    return summary;
  }

  @Override
  public Sources getSources() {
    Sources sources = new Sources();
    EntityCursor<BerkeleyDbSource> cursor = sourceIndex.entities();
    for (BerkeleyDbSource source : cursor) {
      try {
        sources.getSource().add(source.asSource());
      }
      catch (JAXBException e) {
        this.logger.warning(UNABLE_TO_PARSE_PROPERTY_XML + StackTrace.toString(e));
      }
    }
    
    cursor.close();
    return sources;
  }

  @Override
  public List<StraddleList> getStraddleLists(String sourceName,
      List<XMLGregorianCalendar> timestampList) {
    if ((sourceName == null) || (timestampList == null)) {
      return null;
    }
    Source baseSource = getSource(sourceName);
    if (baseSource == null) {
      return null;
    }
    // Want to go through sensordata for base source, and all subsources recursively
    List<Source> sourceList = getAllNonVirtualSubSources(baseSource);
    List<StraddleList> masterList = new ArrayList<StraddleList>(sourceList.size());
    List<SensorDataStraddle> straddleList;
    for (Source subSource : sourceList) {
      straddleList = new ArrayList<SensorDataStraddle>(timestampList.size());
      String subSourceName = subSource.getName();
      for (XMLGregorianCalendar timestamp : timestampList) {
        SensorDataStraddle straddle = getSensorDataStraddle(subSourceName, timestamp);
        if (straddle == null) {
          // No straddle for this timestamp on this source, abort
          return null;
        }
        else {
          straddleList.add(straddle);
        }
      }
      if (straddleList.isEmpty()) {
        return null;
      }
      else {
        masterList.add(new StraddleList(subSource, straddleList));
      }
    }
    return masterList;
  }

  @Override
  public User getUser(String username) {
    if (username == null) {
      return null;
    }
    
    BerkeleyDbUser user = userIndex.get(username);
    if (user == null) {
      return null;
    }
    
    try {
      return user.asUser();
    }
    catch (JAXBException e) {
      this.logger.warning(UNABLE_TO_PARSE_PROPERTY_XML + StackTrace.toString(e));
      return null;
    }
  }

  @Override
  public UserIndex getUsers() {
    UserIndex index = new UserIndex();
    EntityCursor<BerkeleyDbUser> cursor = userIndex.entities();
    for (BerkeleyDbUser user : cursor) {
      index.getUserRef().add(new UserRef(user.getUsername(), this.server));
    }
    cursor.close();
    return index;
  }

  @Override
  public boolean hasSensorData(String sourceName, XMLGregorianCalendar timestamp) {
    return this.getSensorData(sourceName, timestamp) != null;
  }

  @Override
  public boolean indexTables() {
    // No need to index.
    return true;
  }

  @Override
  public boolean makeSnapshot() {
    //Use the DbBackup helper class to backup our database.
    //See: http://download.oracle.com/docs/cd/E17277_02/html/GettingStartedGuide/backup.html#dbbackuphelper
    DbBackup backupHelper = new DbBackup(this.environment, this.lastBackupFileId);
    
    //Determine what was the last backup file.
    boolean success = false;
    backupHelper.startBackup();
    try {
      String[] filesForBackup = backupHelper.getLogFilesInBackupSet();
      success = this.writeBackup(filesForBackup);
      
      //Update our last known backup file.
      this.lastBackupFileId = backupHelper.getLastFileInBackupSet();
    }
    finally {
      // Exit backup mode to clean up.
      backupHelper.endBackup();
    }
    return success;
  }

  @Override
  public boolean storeSensorData(SensorData data) {
    if (data == null) {
      return false;
    }
    
    BerkeleyDbSensorData sensorData;
    if (data.isSetProperties()) {
      try {
        sensorData = new BerkeleyDbSensorData(data.getTimestamp(), 
            data.getTool(), data.getSource(), data.getProperties());
      }
      catch (JAXBException e) {
        this.logger.warning(UNABLE_TO_PARSE_PROPERTY_XML + StackTrace.toString(e));
        sensorData = new BerkeleyDbSensorData(data.getTimestamp(), 
            data.getTool(), data.getSource());
      }
    }
    else {
      sensorData = new BerkeleyDbSensorData(data.getTimestamp(), 
          data.getTool(), data.getSource());
    }
    return sensorDataIndex.putNoOverwrite(sensorData);
  }

  @Override
  public boolean storeSource(Source source, boolean overwrite) {
    if (source == null) {
      return false;
    }
    
    BerkeleyDbSource dbSource = new BerkeleyDbSource(source.getName(), source.getOwner(), source.isPublic(), 
        source.isVirtual(), source.getLocation(), source.getDescription(), source.getCoordinates());
    
    if (source.isSetProperties()) {
      try {
        dbSource.setProperties(source.getProperties());
      }
      catch (JAXBException e) {
        this.logger.warning(UNABLE_TO_PARSE_PROPERTY_XML + StackTrace.toString(e));
      }
    }
    
    if (source.isSetSubSources()) {
      try {
        dbSource.setSubSources(source.getSubSources());
      }
      catch (JAXBException e) {
        this.logger.warning(UNABLE_TO_PARSE_PROPERTY_XML + StackTrace.toString(e));
      }
    }
    
    if (!overwrite) {
      return sourceIndex.putNoOverwrite(dbSource);
    }
    
    sourceIndex.put(dbSource);
    return true;
  }

  @Override
  public boolean storeUser(User user) {
    if (user == null) {
      return false;
    }
    
    BerkeleyDbUser dbUser;
    if (user.isSetProperties()) {
      try {
        dbUser = new BerkeleyDbUser(user.getEmail(), user.getPassword(), 
            user.isAdmin(), user.getProperties());
      }
      catch (JAXBException e) {
        this.logger.warning(UNABLE_TO_PARSE_PROPERTY_XML + StackTrace.toString(e));
        dbUser = new BerkeleyDbUser(user.getEmail(), user.getPassword(), 
            user.isAdmin());
      }
    }
    else {
      dbUser = new BerkeleyDbUser(user.getEmail(), user.getPassword(), 
          user.isAdmin());
    }
    
    boolean success = userIndex.putNoOverwrite(dbUser);
    if (!success) {
      this.logger.fine("BerkeleyDB: Attempted to overwrite User " + user.getEmail());
    }
    
    return success;
  }

  @Override
  public boolean wipeData() {
    EntityCursor<BerkeleyDbSensorData> sensorDataCursor = sensorDataIndex.entities();
    while (sensorDataCursor.next() != null) {
      sensorDataCursor.delete();
    }
    sensorDataCursor.close();
    
    EntityCursor<BerkeleyDbSource> sourceCursor = sourceIndex.entities();
    while (sourceCursor.next() != null) {
      sourceCursor.delete();
    }
    sourceCursor.close();
    
    EntityCursor<BerkeleyDbUser> userCursor = userIndex.entities();
    while (userCursor.next() != null) {
      userCursor.delete();
    }
    userCursor.close();
    
    return true;
  }

  /**
   * Write the files to the backup folder.
   * 
   * @param filenames List of files to backup.
   * @return True if the backup is successful, false otherwise.
   */
  private boolean writeBackup(String[] filenames) {
    boolean success = false;
    File sourceFile, destFile;
    FileChannel source = null;
    FileChannel dest = null;
    
    if (filenames.length == 0) {
      //Nothing to back up.
      return true;
    }
    
    for (String filename : filenames) {
      // Filenames are rooted in berkeleyDb folder.
      sourceFile = new File("berkeleyDb", filename);
      destFile = new File(this.backupDir, sourceFile.getName());
      
      try {
        source = new FileInputStream(sourceFile).getChannel();
        dest = new FileOutputStream(destFile).getChannel();
        dest.transferFrom(source, 0, source.size());
        success = true;
      }
      catch (FileNotFoundException e) {
        this.logger.warning("BerkeleyDB: Could not backup file " + filename);
      }
      catch (IOException e) {
        this.logger.warning("BerkeleyDB: Could not copy file " + filename);
      }
      finally {
        try {
          if (source != null) {
            source.close();
          }
          if (dest != null) {
            dest.close();
          }
        }
        catch (IOException e) {
          this.logger.warning("BerkeleyDB: Could not close source and dest channels.");
        }
      }
    }
    return success;
  }

  @Override
  public boolean performMaintenance() {
    // Apparently, there's no need for me to manage the compression of the database.
    // See: http://download.oracle.com/docs/cd/E17277_02/html/GettingStartedGuide/backgroundthreads.html
    return true;
  }
}
