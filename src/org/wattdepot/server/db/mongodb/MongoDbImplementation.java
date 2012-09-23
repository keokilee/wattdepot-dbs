package org.wattdepot.server.db.mongodb;

import java.io.StringReader;
import java.io.StringWriter;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.datatype.DatatypeConstants;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.resource.property.jaxb.Properties;
import org.wattdepot.resource.sensordata.SensorDataStraddle;
import org.wattdepot.resource.sensordata.StraddleList;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.sensordata.jaxb.SensorDataIndex;
import org.wattdepot.resource.sensordata.jaxb.SensorDataRef;
import org.wattdepot.resource.sensordata.jaxb.SensorDatas;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.resource.source.jaxb.SourceIndex;
import org.wattdepot.resource.source.jaxb.SourceRef;
import org.wattdepot.resource.source.jaxb.Sources;
import org.wattdepot.resource.source.jaxb.SubSources;
import org.wattdepot.resource.source.summary.jaxb.SourceSummary;
import org.wattdepot.resource.user.jaxb.User;
import org.wattdepot.resource.user.jaxb.UserIndex;
import org.wattdepot.resource.user.jaxb.UserRef;
import org.wattdepot.server.Server;
import org.wattdepot.server.ServerProperties;
import org.wattdepot.server.db.DbBadIntervalException;
import org.wattdepot.server.db.DbImplementation;
import org.wattdepot.util.StackTrace;
import org.wattdepot.util.tstamp.Tstamp;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

/**
 * Provides an implementation of DbImplementation that uses MongoDB, a NoSQL database.
 * 
 * @author George Lee
 *
 */
public class MongoDbImplementation extends DbImplementation {
  private static final String UNABLE_TO_PARSE_PROPERTY_XML =
    "Unable to parse property XML from database ";
  
  private static final String SOURCE_KEY = "source";
  private static final String TIMESTAMP_KEY = "timestamp";
  private static final String NAME_KEY = "name";
  private static final String PROPERTIES_KEY = "properties";

  private static JAXBContext subSourcesJAXB;

  private boolean isFreshlyCreated;
  private DBCollection sensorDataCollection;
  private DBCollection sourceCollection;
  private DBCollection userCollection;
  
  /** Property JAXBContext. */
  private static final JAXBContext propertiesJAXB;
  
  static {
    try {
      propertiesJAXB = JAXBContext.newInstance(org.wattdepot.resource.property.jaxb.Properties.class);
      subSourcesJAXB = JAXBContext.newInstance(org.wattdepot.resource.source.jaxb.SubSources.class);
    }
    catch (Exception e) {
      throw new RuntimeException("Couldn't create JAXB context instance.", e);
    }
  }

  /**
   * Instantiates this implementation.
   * 
   * @param server The server this implementation is associated with.
   */
  public MongoDbImplementation(Server server) {
    super(server);
    // TODO Auto-generated constructor stub
  }

  @Override
  public boolean deleteSensorData(String sourceName, XMLGregorianCalendar timestamp) {
    if (sourceName == null || timestamp == null) {
      return false;
    }
    
    String sourceUri = Source.sourceToUri(sourceName, this.server);
    BasicDBObject query = new BasicDBObject(SOURCE_KEY, sourceUri);
    query.put(TIMESTAMP_KEY, timestamp.toGregorianCalendar().getTimeInMillis());
    
    DBObject object = this.sensorDataCollection.findAndRemove(query);
    
    // Deleted object should be returned.
    return object != null;
  }

  @Override
  public boolean deleteSensorData(String sourceName) {
    if (sourceName == null) {
      return false;
    }
    
    String sourceUri = Source.sourceToUri(sourceName, this.server);
    BasicDBObject query = new BasicDBObject(SOURCE_KEY, sourceUri);
    WriteResult result = this.sensorDataCollection.remove(query, WriteConcern.SAFE);
    
    // The only way we can tell if anything was deleted is to look at the number of affected records
    return result.getN() > 0;
  }

  @Override
  public boolean deleteSource(String sourceName) {
    if (sourceName == null) {
      return false;
    }
    
    DBObject object = this.sourceCollection.findAndRemove(new BasicDBObject(NAME_KEY, sourceName));
    return object != null;
  }

  @Override
  public boolean deleteUser(String username) {
    if (username == null) {
      return false;
    }
    
    DBObject object = this.userCollection.findAndRemove(new BasicDBObject(NAME_KEY, username));
    return object != null;
  }

  @Override
  protected SensorData getLatestNonVirtualSensorData(String sourceName) {
    if (sourceName == null) {
      return null;
    }
    
    String source = Source.sourceToUri(sourceName, this.server);
    DBCursor cursor = this.sensorDataCollection.find(new BasicDBObject(SOURCE_KEY, source));
    if (cursor.count() == 0) {
      return null;
    }
    
    cursor = cursor.sort(new BasicDBObject(TIMESTAMP_KEY, -1)).limit(1);
    return this.dbObjectToSensorData(cursor.next());
  }
  
  /**
   * Converts a DBObject from MongoDB to WattDepot sensor data.
   * 
   * @param object The sensor data retrieved from MongoDB.
   * @return The converted WattDepot SensorData.
   */
  private SensorData dbObjectToSensorData(DBObject object) {
    if (object == null) {
      return null;
    }
    
    SensorData data = new SensorData();
    data.setSource((String) object.get(SOURCE_KEY));
    data.setTimestamp(Tstamp.makeTimestamp((Long)object.get(TIMESTAMP_KEY)));
    data.setTool((String) object.get("tool"));
    
    if (object.containsField(PROPERTIES_KEY)) {
      String props = (String)object.get(PROPERTIES_KEY);
      try {
        Unmarshaller unmarshaller = propertiesJAXB.createUnmarshaller();
        data.setProperties((Properties) unmarshaller.unmarshal(new StringReader(props)));
      }
      catch (JAXBException e) {
        this.logger.warning(UNABLE_TO_PARSE_PROPERTY_XML + StackTrace.toString(e));
      }
    }
    
    return data;
  }
  
  @Override
  public SensorData getSensorData(String sourceName, XMLGregorianCalendar timestamp) {
    if (sourceName == null || timestamp == null) {
      return null;
    }
    
    BasicDBObject query = new BasicDBObject();
    query.put(SOURCE_KEY, Source.sourceToUri(sourceName, this.server));
    query.put(TIMESTAMP_KEY, timestamp.toGregorianCalendar().getTimeInMillis());
    DBObject object = this.sensorDataCollection.findOne(query);
    
    if (object == null) {
      return null;
    }
    
    return this.dbObjectToSensorData(object);
  }

  @Override
  public SensorDataIndex getSensorDataIndex(String sourceName) {
    try {
      return this.getSensorDataIndex(sourceName, Tstamp.makeTimestamp(0), Tstamp.makeTimestamp());
    }
    catch (DbBadIntervalException e) {
      // Should not happen
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
    
    //Construct the query
    SensorDataIndex index = new SensorDataIndex();
    String sourceUri = Source.sourceToUri(sourceName, this.server.getHostName());
    Long start = startTime.toGregorianCalendar().getTimeInMillis();
    Long end = endTime.toGregorianCalendar().getTimeInMillis();
    
    BasicDBObject query = new BasicDBObject();
    query.put(SOURCE_KEY, sourceUri);
    // Mongo uses "gte" and "lte" for greater/less than or equal
    query.put(TIMESTAMP_KEY, new BasicDBObject("$gte", start).append("$lte", end));
    
    DBCursor cursor = this.sensorDataCollection.find(query);
    SensorDataRef ref;
    for (DBObject object : cursor) {
      ref = new SensorDataRef(Tstamp.makeTimestamp((Long)object.get(TIMESTAMP_KEY)), 
          (String) object.get("tool"), (String) object.get(SOURCE_KEY));
      index.getSensorDataRef().add(ref);
    }
    return index;
  }

  @Override
  public SensorDataStraddle getSensorDataStraddle(String sourceName, XMLGregorianCalendar timestamp) {
    if (sourceName == null || timestamp == null) {
      return null;
    }
    else if (this.getSource(sourceName) == null) {
      return null;
    }
    
    SensorData data = this.getSensorData(sourceName, timestamp);
    if (data == null) {
      DBCursor cursor;
      DBObject dbData;
      SensorData beforeData, afterData;
      long dbTime = timestamp.toGregorianCalendar().getTimeInMillis();
      
      String sourceUri = Source.sourceToUri(sourceName, this.server.getHostName());
      
      // Grab data immediately previous.
      BasicDBObject query = new BasicDBObject(SOURCE_KEY, sourceUri);
      query.put(TIMESTAMP_KEY, new BasicDBObject("$lt", dbTime));
      cursor = this.sensorDataCollection.find(query);
      cursor.sort(new BasicDBObject(TIMESTAMP_KEY, -1)).limit(1);
      if (cursor.count() == 0) {
        return null;
      }
      dbData = cursor.next();
      cursor.close();
      if (dbData == null) {
        return null; //No straddle
      }
      beforeData = this.dbObjectToSensorData(dbData);
      
      //Grab data immediately after.
      query = new BasicDBObject(SOURCE_KEY, sourceUri);
      query.put(TIMESTAMP_KEY, new BasicDBObject("$gt", dbTime));
      cursor = this.sensorDataCollection.find(query);
      if (cursor.count() == 0) {
        return null;
      }
      cursor.sort(new BasicDBObject(TIMESTAMP_KEY, 1)).limit(1);
      dbData = cursor.next();
      cursor.close();
      if (dbData == null) {
        return null; //No straddle
      }
      afterData = this.dbObjectToSensorData(dbData);
      
      return new SensorDataStraddle(timestamp, beforeData, afterData);
    }
    
    // At this point, we have data for this timestamp so return the data twice.
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
    if (sourceName == null || startTime == null || endTime == null) {
      return null;
    }
    else if (this.getSource(sourceName) == null) {
      return null;
    }
    
    String sourceUri = Source.sourceToUri(sourceName, server);
    BasicDBObject query = new BasicDBObject(SOURCE_KEY, sourceUri);
    long start = startTime.toGregorianCalendar().getTimeInMillis();
    long end = endTime.toGregorianCalendar().getTimeInMillis();
    BasicDBObject range = new BasicDBObject();
    range.put("$gte", start);
    range.put("$lte", end);
    query.put(TIMESTAMP_KEY, range);
    DBCursor cursor = this.sensorDataCollection.find(query);
    SensorDatas datas = new SensorDatas();
    for (DBObject object : cursor) {
      datas.getSensorData().add(this.dbObjectToSensorData(object));
    }
    
    return datas;
  }

  /**
   * Converts the DBObject retrieved from MongoDB to a WattDepot source object.
   * 
   * @param object The object retrieved from MongoDB.
   * @return The converted WattDepot Source object.
   */
  private Source dbObjectToSource(DBObject object) {
    Source source = new Source();
    source.setName((String)object.get(NAME_KEY));
    source.setOwner((String)object.get("owner"));
    source.setPublic((Boolean)object.get("isPublic"));
    source.setVirtual((Boolean)object.get("isVirtual"));
    source.setLocation((String)object.get("location"));
    source.setCoordinates((String)object.get("coordinates"));
    source.setDescription((String)object.get("description"));
    if (object.containsField(PROPERTIES_KEY)) {
      String props = (String)object.get(PROPERTIES_KEY);
      try {
        Unmarshaller unmarshaller = propertiesJAXB.createUnmarshaller();
        source.setProperties((Properties) unmarshaller.unmarshal(new StringReader(props)));
      }
      catch (JAXBException e) {
        this.logger.warning(UNABLE_TO_PARSE_PROPERTY_XML + StackTrace.toString(e));
      }
    }
    
    if (object.containsField("subSources")) {
      String subSources = (String)object.get("subSources");
      try {
        Unmarshaller unmarshaller = subSourcesJAXB.createUnmarshaller();
        source.setSubSources((SubSources) unmarshaller.unmarshal(new StringReader(subSources)));
      }
      catch (JAXBException e) {
        this.logger.warning("Unable to parse SubSource XML " + StackTrace.toString(e));
      }
    }
    
    return source;
  }
  
  /**
   * Converts the MongoDB object to a WattDepot SourceRef.
   * 
   * @param object The object received from WattDepot.
   * @return The converted WattDepot SourceRef.
   */
  private SourceRef dbObjectToSourceRef(DBObject object) {
    SourceRef ref = new SourceRef();
    ref.setName((String) object.get(NAME_KEY));
    ref.setOwner((String) object.get("owner"));
    ref.setCoordinates((String) object.get("coordinates"));
    ref.setDescription((String)object.get("description"));
    ref.setLocation((String) object.get("location"));
    ref.setPublic((Boolean) object.get("isPublic"));
    ref.setVirtual((Boolean) object.get("isVirtual"));
    ref.setHref((String) object.get(NAME_KEY), this.server);
    return ref;
  }
  
  @Override
  public Source getSource(String sourceName) {
    if (sourceName == null) {
      return null;
    }
    
    BasicDBObject query = new BasicDBObject(NAME_KEY, sourceName);
    DBObject dbSource = this.sourceCollection.findOne(query);
    if (dbSource == null) {
      return null;
    }
    
    return this.dbObjectToSource(dbSource);
  }

  @Override
  public SourceIndex getSourceIndex() {
    SourceIndex index = new SourceIndex();
    DBCursor cursor = this.sourceCollection.find().sort(new BasicDBObject(NAME_KEY, 1));
    for (DBObject object : cursor) {
      index.getSourceRef().add(this.dbObjectToSourceRef(object));
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
    Long firstTimestamp = null, lastTimestamp = null;
    DBCursor cursor;
    DBObject temp;
    List<DBObject> sensorData;
    int dataCount = 0;
    String subsourceUri;
    for (Source subSource : sourceList) {
      subsourceUri = Source.sourceToUri(subSource.getName(), this.server.getHostName());
      //Create cursor for getting data.
      cursor = this.sensorDataCollection.find(new BasicDBObject(SOURCE_KEY, subsourceUri));
      sensorData = cursor.toArray();
      
      //Count the number of results for this source.
      dataCount += sensorData.size();
      if (dataCount > 0) {
        //Get first timestamp of sensor data.
        temp = sensorData.get(0);
        if (firstTimestamp == null || (Long)temp.get(TIMESTAMP_KEY) < firstTimestamp) {
          firstTimestamp = (Long)temp.get(TIMESTAMP_KEY);
        }
        
        //Get last timestamp of sensor data.
        temp = sensorData.get(sensorData.size() - 1);
        if (lastTimestamp == null || (Long)temp.get(TIMESTAMP_KEY) > lastTimestamp) {
          lastTimestamp = (Long)temp.get(TIMESTAMP_KEY);
        }
      }
      //Clean up
      cursor.close();
    }
    
    summary.setFirstSensorData(Tstamp.makeTimestamp(firstTimestamp));
    summary.setLastSensorData(Tstamp.makeTimestamp(lastTimestamp));
    summary.setTotalSensorDatas(dataCount);
    return summary;
  }

  @Override
  public Sources getSources() {
    Sources sources = new Sources();
    DBCursor cursor = this.sourceCollection.find().sort(new BasicDBObject(NAME_KEY, 1));
    for (DBObject object : cursor) {
      sources.getSource().add(this.dbObjectToSource(object));
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

  /**
   * Converts a MongoDB object to a WattDepot User object.
   * 
   * @param object The object received from MongoDB.
   * @return The converted WattDepot User object.
   */
  private User dbObjectToUser(DBObject object) {
    User user = new User();
    user.setEmail((String) object.get(NAME_KEY));
    user.setPassword((String) object.get("password"));
    user.setAdmin((Boolean) object.get("isAdmin"));
    
    if (object.containsField(PROPERTIES_KEY)) {
      String props = (String)object.get(PROPERTIES_KEY);
      try {
        Unmarshaller unmarshaller = propertiesJAXB.createUnmarshaller();
        user.setProperties((Properties) unmarshaller.unmarshal(new StringReader(props)));
      }
      catch (JAXBException e) {
        this.logger.warning(UNABLE_TO_PARSE_PROPERTY_XML + StackTrace.toString(e));
      }
    }
    
    return user;
  }
  
  @Override
  public User getUser(String username) {
    if (username == null) {
      return null;
    }
    
    BasicDBObject query = new BasicDBObject(NAME_KEY, username);
    DBObject object = this.userCollection.findOne(query);
    if (object == null) {
      return null;
    }
    
    return this.dbObjectToUser(object);
  }

  @Override
  public UserIndex getUsers() {
    UserIndex index = new UserIndex();
    DBCursor cursor = this.userCollection.find().sort(new BasicDBObject(NAME_KEY, 1));
    for (DBObject object : cursor) {
      index.getUserRef().add(new UserRef((String) object.get(NAME_KEY), this.server));
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
    BasicDBObject dataIndex = new BasicDBObject();
    dataIndex.put(SOURCE_KEY, 1);
    dataIndex.put(TIMESTAMP_KEY, 1);
    BasicDBObject uniqueOption = new BasicDBObject("unique", true);
    try {
      this.sensorDataCollection.ensureIndex(dataIndex, uniqueOption);
    }
    catch (MongoException e) {
      this.logger.fine("Index exists on sensor data.");
    }
    
    try {
      this.sourceCollection.ensureIndex(new BasicDBObject(NAME_KEY, 1), uniqueOption);
    }
    catch (MongoException e) {
      this.logger.fine("Index exists on sources.");
    }
    
    try {
      this.userCollection.ensureIndex(new BasicDBObject(NAME_KEY, 1), uniqueOption);
    }
    catch (MongoException e) {
      this.logger.fine("Index exists on users.");
    }
    
    return true;
  }

  @Override
  public void initialize(boolean wipe) {
    ServerProperties props = this.server.getServerProperties();
    String mongoServer = props.get(ServerProperties.MONGODB_HOSTNAME_KEY);
    Integer mongoPort = Integer.parseInt(props.get(ServerProperties.MONGODB_PORT_KEY));
    Mongo mongo;
    
    try {
      mongo = new Mongo(mongoServer, mongoPort);
    }
    catch (UnknownHostException e) {
      this.logger.severe("Could not connect to " + mongoServer);
      throw new RuntimeException("Could not connect. " + StackTrace.toString(e));
    }
    catch (MongoException e) {
      this.logger.severe("Could not connect to "  + mongoServer + "on port " + mongoPort.toString());
      throw new RuntimeException("Could not connect. " + StackTrace.toString(e));
    }
    
    //Check if the database exists.
    String mongoDbName = "wattdepot";
    List<String> mongoDbs = mongo.getDatabaseNames();
    this.isFreshlyCreated = !mongoDbs.contains(mongoDbName);
    DB mongoDb = mongo.getDB(mongoDbName);
    
    //Set up collections and indices.
    this.sensorDataCollection = mongoDb.getCollection("sensorData");
    this.sourceCollection = mongoDb.getCollection("sources");
    this.userCollection = mongoDb.getCollection("users");
    this.indexTables();
    
    //Create shutdown hook.
    DbShutdownHook shutdownHook = new DbShutdownHook(mongo);
    Runtime.getRuntime().addShutdownHook(shutdownHook);
    
    if (wipe) {
      this.wipeData();
    }
  }

  @Override
  public boolean isFreshlyCreated() {
    return this.isFreshlyCreated;
  }

  @Override
  public boolean makeSnapshot() {
    // Server should be backed up separately outside of WattDepot.
    return true;
  }

  @Override
  public boolean performMaintenance() {
    // Server should be backed up separately outside of WattDepot.
    return true;
  }

  @Override
  public boolean storeSensorData(SensorData data) {
    if (data == null) {
      return false;
    }
    
    BasicDBObject dbData = new BasicDBObject();
    dbData.put(SOURCE_KEY, data.getSource());
    dbData.put(TIMESTAMP_KEY, data.getTimestamp().toGregorianCalendar().getTimeInMillis());
    dbData.put("tool", data.getTool());
    dbData.put("lastMod", System.currentTimeMillis());
    
    if (data.isSetProperties()) {
      try {
        Marshaller propertiesMarshaller = propertiesJAXB.createMarshaller();
        StringWriter writer = new StringWriter();
        propertiesMarshaller.marshal(data.getProperties(), writer);
        dbData.put(PROPERTIES_KEY, writer.toString());
      }
      catch (JAXBException e) {
        this.logger.warning(UNABLE_TO_PARSE_PROPERTY_XML + StackTrace.toString(e));
      }
    }
    
    try {
      this.sensorDataCollection.insert(dbData, WriteConcern.SAFE);
    }
    catch (MongoException.DuplicateKey dke) {
      return false;
    }
    
    return true;
  }

  @Override
  public boolean storeSource(Source source, boolean overwrite) {
    if (source == null) {
      return false;
    }
    
    BasicDBObject dbSource = new BasicDBObject();
    dbSource.put(NAME_KEY, source.getName());
    dbSource.put("owner", source.getOwner());
    dbSource.put("description", source.getDescription());
    dbSource.put("location", source.getLocation());
    dbSource.put("coordinates", source.getCoordinates());
    dbSource.put("isPublic", source.isPublic());
    dbSource.put("isVirtual", source.isVirtual());
    dbSource.put("lastMod", System.currentTimeMillis());
    
    if (source.isSetProperties()) {
      try {
        Marshaller propertiesMarshaller = propertiesJAXB.createMarshaller();
        StringWriter writer = new StringWriter();
        propertiesMarshaller.marshal(source.getProperties(), writer);
        dbSource.put(PROPERTIES_KEY, writer.toString());
      }
      catch (JAXBException e) {
        this.logger.warning(UNABLE_TO_PARSE_PROPERTY_XML + StackTrace.toString(e));
      }
    }
    if (source.isSetSubSources()) {
      try {
        Marshaller subSourcesMarshaller = subSourcesJAXB.createMarshaller();
        StringWriter writer = new StringWriter();
        subSourcesMarshaller.marshal(source.getSubSources(), writer);
        dbSource.put("subSources", writer.toString());
      }
      catch (JAXBException e) {
        this.logger.warning("Unable to parse subsources " + StackTrace.toString(e));
      }
    }
    
    if (overwrite) {
      //Find an existing source with this name.
      BasicDBObject query = new BasicDBObject(NAME_KEY, source.getName());
      //Third parameter is to "upsert" (update if exists, insert otherwise).
      //Fourth is to update all matching objects, which shouldn't happen.
      this.sourceCollection.update(query, dbSource, true, false, WriteConcern.SAFE);
    }
    else {
      try {
        this.sourceCollection.insert(dbSource, WriteConcern.SAFE);
      }
      catch (MongoException.DuplicateKey dke) {
        return false;
      }
    }
    
    return true;
  }

  @Override
  public boolean storeUser(User user) {
    if (user == null) {
      return false;
    }
    
    BasicDBObject dbUser = new BasicDBObject();
    dbUser.put(NAME_KEY, user.getEmail());
    dbUser.put("password", user.getPassword());
    dbUser.put("isAdmin", user.isAdmin());
    dbUser.put("lastMod", System.currentTimeMillis());
    
    if (user.isSetProperties()) {
      try {
        Marshaller propertiesMarshaller = propertiesJAXB.createMarshaller();
        StringWriter writer = new StringWriter();
        propertiesMarshaller.marshal(user.getProperties(), writer);
        dbUser.put(PROPERTIES_KEY, writer.toString());
      }
      catch (JAXBException e) {
        this.logger.warning(UNABLE_TO_PARSE_PROPERTY_XML + StackTrace.toString(e));
      }
    }
    
    try {
      this.userCollection.insert(dbUser, WriteConcern.SAFE);
    }
    catch (MongoException.DuplicateKey dke) {
      this.logger.warning("MongoDB: Attempt to overwrite user " + user.getEmail());
      return false;
    }
    
    return true;
  }

  @Override
  public boolean wipeData() {
    this.sensorDataCollection.drop();
    this.sourceCollection.drop();
    this.userCollection.drop();
    
    //drop() drops indexes, so we should rebuild them here.
    this.indexTables();
    
    return true;
  }

}
