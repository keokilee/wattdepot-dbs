package org.wattdepot.server.db.berkeleydb;

import java.io.StringReader;
import java.io.StringWriter;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.resource.property.jaxb.Properties;
import org.wattdepot.resource.user.jaxb.User;
import org.wattdepot.util.tstamp.Tstamp;
import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

/**
 * Represents a WattDepot user in BerkeleyDB.
 * 
 * @author George Lee
 *
 */
@Entity
public class BerkeleyDbUser {
  @PrimaryKey
  private String username;
  private String password;
  private boolean isAdmin;
  private String properties;
  private long lastMod;
  
  /** Property JAXBContext. */
  private static final JAXBContext propertiesJAXB;

  // JAXBContexts are thread safe, so we can share them across all instances and threads.
  // https://jaxb.dev.java.net/guide/Performance_and_thread_safety.html
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
   * Default constructor required by BerkeleyDb.
   */
  public BerkeleyDbUser() {
    //Required by BerkeleyDb.
  }
  
  /**
   * Construct a BerkeleyDbUser given the properties from a WattDepot user.
   * 
   * @param email The email of the user (their username).
   * @param password The user's password.
   * @param isAdmin True if the user is an admin, false otherwise.
   */
  public BerkeleyDbUser(String email, String password, boolean isAdmin) {
    this.username = email;
    this.password = password;
    this.isAdmin = isAdmin;
    this.lastMod = Tstamp.makeTimestamp().toGregorianCalendar().getTimeInMillis();
  }
  
  /**
   * Construct a BerkeleyDbUser given the properties from a WattDepot user.
   * 
   * @param email The email of the user (their username).
   * @param password The user's password.
   * @param isAdmin True if the user is an admin, false otherwise.
   * @param properties Properties associated with this user.
   * @throws JAXBException if marshaller cannot be instantiated or marshal properties.
   */
  public BerkeleyDbUser(String email, String password, boolean isAdmin, Properties properties) throws JAXBException {
    Marshaller marshaller = null;
    marshaller = propertiesJAXB.createMarshaller();
    this.username = email;
    this.password = password;
    this.isAdmin = isAdmin;
    this.lastMod = Tstamp.makeTimestamp().toGregorianCalendar().getTimeInMillis();
    if (properties != null) {
      StringWriter writer = new StringWriter();
      marshaller.marshal(properties, writer);
      this.properties = writer.toString();
    }
  }

  /**
   * Get the username of this user.
   * @return The user's username (their email address).
   */
  public String getUsername() {
    return this.username;
  }
  
  /**
   * Get a WattDepot representation of this BerkeleyDbUser.
   * @return WattDepot representation of this user.
   * @throws JAXBException if the properties cannot be unmarshalled.
   */
  public User asUser() throws JAXBException {
    User user = new User();
    user.setEmail(this.username);
    user.setPassword(this.password);
    user.setAdmin(this.isAdmin);
    if (this.properties != null) {
      Unmarshaller unmarshaller = propertiesJAXB.createUnmarshaller();
      user.setProperties((Properties) unmarshaller.unmarshal(new StringReader(this.properties)));
    }
    return user;
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
