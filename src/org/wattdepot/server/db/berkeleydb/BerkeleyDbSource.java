package org.wattdepot.server.db.berkeleydb;

import java.io.StringReader;
import java.io.StringWriter;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.resource.property.jaxb.Properties;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.resource.source.jaxb.SourceRef;
import org.wattdepot.resource.source.jaxb.SubSources;
import org.wattdepot.server.Server;
import org.wattdepot.util.tstamp.Tstamp;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

/**
 * Represents a source entity for use in BerkeleyDB.
 * 
 * @author George Lee
 *
 */
@Entity
public class BerkeleyDbSource {
  @PrimaryKey
  private String name;
  private String owner;
  private boolean isPublic;
  private boolean isVirtual;
  private String location;
  private String coordinates;
  private String description;
  private String subsources;
  private String properties;
  private long lastMod;

  /** Property JAXBContext. */
  private static final JAXBContext propertiesJAXB;
  /** SubSources JAXBContext. */
  private static final JAXBContext subSourcesJAXB;

  // JAXBContexts are thread safe, so we can share them across all instances and threads.
  // https://jaxb.dev.java.net/guide/Performance_and_thread_safety.html
  static {
    try {
      propertiesJAXB =
          JAXBContext.newInstance(org.wattdepot.resource.property.jaxb.Properties.class);
      subSourcesJAXB = JAXBContext.newInstance(org.wattdepot.resource.source.jaxb.SubSources.class);
    }
    catch (Exception e) {
      throw new RuntimeException("Couldn't create JAXB context instance.", e);
    }
  }
  
  /**
   * Default constructor as required by BerkeleyDb.
   */
  public BerkeleyDbSource() {
    // Required by BerkeleyDb.
  }
  
  /**
   * Construct a BerkeleyDbSource given the source parameters.
   * 
   * @param name The name of the source.
   * @param owner The owner of the source.
   * @param isPublic True if the source is public, false otherwise.
   * @param isVirtual True if the source is virtual, false otherwise.
   * @param location The location of the source.
   * @param description The description of the source.
   * @param coordinates The coordinates of the source.
   */
  public BerkeleyDbSource(String name, String owner, boolean isPublic, boolean isVirtual, String location, 
      String description, String coordinates) {
    this.name = name;
    this.owner = owner;
    this.isPublic = isPublic;
    this.location = location;
    this.description = description;
    this.coordinates = coordinates;
    this.isVirtual = isVirtual;
    this.lastMod = Tstamp.makeTimestamp().toGregorianCalendar().getTimeInMillis();
  }
  
  /**
   * Set the subsources for this source.
   * @param subSources The subsources of this source.
   * @throws JAXBException if the subsources cannot be parsed.
   */
  public void setSubSources(SubSources subSources) throws JAXBException {
    Marshaller subSourcesMarshaller = subSourcesJAXB.createMarshaller();
    StringWriter writer = new StringWriter();
    subSourcesMarshaller.marshal(subSources, writer);
    this.subsources = writer.toString();
  }
  
  /**
   * Set the properties for this source.
   * @param properties The source's properties.
   * @throws JAXBException if the properties cannot be parsed.
   */
  public void setProperties(Properties properties) throws JAXBException {
    Marshaller propertiesMarshaller = propertiesJAXB.createMarshaller();
    StringWriter writer = new StringWriter();
    propertiesMarshaller.marshal(properties, writer);
    this.properties = writer.toString();
  }
  
  /**
   * Get the last modification date of this sensor data.
   * 
   * @return The last modification date.
   */
  public XMLGregorianCalendar getLastMod() {
    return Tstamp.makeTimestamp(this.lastMod);
  }
  
  /**
   * Converts this BerkeleyDB source into a WattDepot source.
   * 
   * @return WattDepot representation of this source.
   * @throws JAXBException if subsources or properties are unable to be unmarshalled.
   */
  public Source asSource() throws JAXBException {
    Source returnSource = new Source();
    returnSource.setName(this.name);
    returnSource.setOwner(this.owner);
    returnSource.setPublic(this.isPublic);
    returnSource.setVirtual(this.isVirtual);
    returnSource.setLocation(this.location);
    returnSource.setDescription(this.description);
    returnSource.setCoordinates(this.coordinates);
    if (this.subsources != null) {
      Unmarshaller unmarshaller = subSourcesJAXB.createUnmarshaller();
      returnSource.setSubSources((SubSources) unmarshaller.unmarshal(new StringReader(this.subsources)));
    }
    
    if (this.properties != null) {
      Unmarshaller unmarshaller = propertiesJAXB.createUnmarshaller();
      returnSource.setProperties((Properties) unmarshaller.unmarshal(new StringReader(this.properties)));
    }
    return returnSource;
  }

  /**
   * Returns a WattDepot SourceRef for this source.
   * 
   * @param server The current server instance.  Used to calculate the Href value.
   * 
   * @return The WattDepot SourceRef for this source.
   */
  public SourceRef asSourceRef(Server server) {
    SourceRef ref = new SourceRef();
    ref.setName(this.name);
    ref.setOwner(this.owner);
    ref.setPublic(this.isPublic);
    ref.setVirtual(this.isVirtual);
    ref.setLocation(this.location);
    ref.setDescription(this.description);
    ref.setHref(name, server);
    ref.setCoordinates(this.coordinates);
    return ref;
  }
}
