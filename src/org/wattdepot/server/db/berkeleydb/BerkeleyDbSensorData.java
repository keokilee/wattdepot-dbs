package org.wattdepot.server.db.berkeleydb;

import java.io.StringReader;
import java.io.StringWriter;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.resource.property.jaxb.Properties;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.util.tstamp.Tstamp;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.KeyField;
import com.sleepycat.persist.model.Persistent;
import com.sleepycat.persist.model.PrimaryKey;

/**
 * Implementation of SensorData that is backed by BerkeleyDB.
 * 
 * @author George Lee
 *
 */
@Entity
public class BerkeleyDbSensorData {
  @PrimaryKey
  private CompositeSensorDataKey key;
  private String tool;
  private String properties;
  private long lastMod;
  
  /** Property JAXBContext. */
  private static final JAXBContext propertiesJAXB;
  
  static {
    try {
      propertiesJAXB =
          JAXBContext.newInstance(org.wattdepot.resource.property.jaxb.Properties.class);
    }
    catch (Exception e) {
      throw new RuntimeException("Couldn't create JAXB context instance.", e);
    }
  }
  
  /**
   * Default constructor as required by BerkeleyDB.
   */
  public BerkeleyDbSensorData() {
    //Required by BerkeleyDB.
  }
  
  /**
   * Create a BerkeleyDbSensorData instance using parameters from SensorData.
   * 
   * @param timestamp The timestamp of the sensor data.
   * @param tool The tool the sensor data was recorded with.
   * @param source The source of the sensor data. 
   */
  public BerkeleyDbSensorData(XMLGregorianCalendar timestamp, String tool, String source) {
    this.key = new CompositeSensorDataKey(source, timestamp);
    this.tool = tool;
    this.lastMod = Tstamp.makeTimestamp().toGregorianCalendar().getTimeInMillis();
  }
  
  /**
   * Create a BerkeleyDbSensorData instance using parameters from SensorData.
   * 
   * @param timestamp The timestamp of the sensor data.
   * @param tool The tool the sensor data was recorded with.
   * @param source The source of the sensor data. 
   * @param properties The properties of the sensor data.
   * @throws JAXBException if the properties are unable to be marshalled.
   */
  public BerkeleyDbSensorData(XMLGregorianCalendar timestamp, String tool, String source, Properties properties) 
      throws JAXBException {
    this(timestamp, tool, source);
    // Try and get the marshaller set up.
    Marshaller propertiesMarshaller = null;
    propertiesMarshaller = propertiesJAXB.createMarshaller();

    if (properties != null) {
      StringWriter writer = new StringWriter();
      propertiesMarshaller.marshal(properties, writer);
      this.properties = writer.toString();
    }
  }

  /**
   * Converts the BerkeleyDB representation of sensor data to the jaxb SensorData.
   * 
   * @return Instance of SensorData with the same properties as this.
   * @throws JAXBException if the properties are unable to be unmarshalled.
   */
  public SensorData asSensorData() throws JAXBException {
    SensorData returnData = new SensorData();
    returnData.setSource(this.key.getSource());
    returnData.setTimestamp(this.getTimestamp());
    returnData.setTool(this.tool);
    if (this.properties != null) {
      Unmarshaller unmarshaller = propertiesJAXB.createUnmarshaller();
      returnData.setProperties((Properties) unmarshaller.unmarshal(new StringReader(this.properties)));
    }
    return returnData;
  }

  /**
   * Get the timestamp associated with this sensor data.
   * 
   * @return The timestamp of this sensor data in XMLGregorianCalendar format.
   */
  public XMLGregorianCalendar getTimestamp() {
    return Tstamp.makeTimestamp(this.key.getTimestamp());
  }

  /**
   * Get the tool associated with this sensor data.
   * 
   * @return The tool associated with this sensor data.
   */
  public String getTool() {
    return this.tool;
  }

  /**
   * Get the source associated with this sensor data.
   * 
   * @return The source assoiated with this sensor data.
   */
  public String getSource() {
    return this.key.getSource();
  }
  
  /**
   * Get the last modification date of this sensor data.
   * 
   * @return The last modification date.
   */
  public XMLGregorianCalendar getLastMod() {
    return Tstamp.makeTimestamp(this.lastMod);
  }
}

/**
 * Represents a composite key for sensor data in BerkeleyDB.
 * @author George Lee
 *
 */
@Persistent
class CompositeSensorDataKey {
  @KeyField(1) private String source;
  @KeyField(2) private long timestamp;
  
  /**
   * Default constructor required by BerkeleyDB.
   */
  CompositeSensorDataKey() {
    //Required by BerkeleyDB.
  }
  
  /**
   * Constructor for our composite key.
   * 
   * @param source The name of the source.
   * @param timestamp The timestamp of the source data.
   */
  CompositeSensorDataKey(String source, XMLGregorianCalendar timestamp) {
    this.timestamp = timestamp.toGregorianCalendar().getTimeInMillis();
    this.source = source;
  }
  
  /**
   * Get the source associated with this key.
   * 
   * @return The source associated with this key.
   */
  String getSource() {
    return this.source;
  }
  
  /**
   * Get the timestamp associated with this key in long format.
   * 
   * @return The timestamp associated with this key represented as a long.
   */
  long getTimestamp() {
    return this.timestamp;
  }
}