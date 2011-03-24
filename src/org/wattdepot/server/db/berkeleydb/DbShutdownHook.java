package org.wattdepot.server.db.berkeleydb;

import java.util.List;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.persist.EntityStore;

/**
 * Provides a shutdown hook specifying code to be run at the end of this application's life.
 * For more details, see: http://onjava.com/pub/a/onjava/2003/03/26/shutdownhook.html
 * @author Philip Johnson, George Lee
 */
public class DbShutdownHook extends Thread {
  /** The environment instance to be closed upon shutdown. */
  private Environment env;
  /** The entity store to be closed upon shutdown. */
  private List<EntityStore> stores;

  /**
   * Called from within the BerkeleyDbImplementation to pass the Environment and EntityStore to be closed.
   * 
   * @param env The Environment.
   * @param stores The EntityStores to be closed. 
   */
  public DbShutdownHook(Environment env, List<EntityStore> stores) {
    this.env = env;
    this.stores = stores;
  }

  /**
   * Runs at system shutdown time and closes the environment and entity store. 
   */
  public void run() {
    try {
      if (env != null) {
        for (EntityStore store : this.stores) {
          store.close();
        }
        env.cleanLog();
        env.close();
        // Comment this next line out during production, but good to see during development.
        System.out.println("Databases closed.");
      } 
    } 
    catch (DatabaseException dbe) {
      System.out.println("Databases not closed successfully.");
    } 
  }
}

