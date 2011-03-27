package org.wattdepot.server.db.mongodb;

import com.mongodb.Mongo;
import com.mongodb.MongoException;

/**
 * Provides a shutdown hook specifying code to be run at the end of this application's life.
 * For more details, see: http://onjava.com/pub/a/onjava/2003/03/26/shutdownhook.html
 * @author Philip Johnson, George Lee
 */
public class DbShutdownHook extends Thread {
  /** The Mongo connection to be closed upon shutdown. */
  private Mongo mongo;

  /**
   * Called from within the MongoDbImplementation to pass the connection to be closed.
   * 
   * @param mongo the MongoDB connection to close.
   */
  public DbShutdownHook(Mongo mongo) {
    this.mongo = mongo;
  }

  /**
   * Runs at system shutdown time and closes the environment and entity store. 
   */
  @Override
  public void run() {
    try {
      if (mongo != null) {
        mongo.close();
        // Comment this next line out during production, but good to see during development.
        System.out.println("Mongo closed.");
      } 
    } 
    catch (MongoException e) {
      System.out.println("Databases not closed successfully.");
    } 
  }
}

