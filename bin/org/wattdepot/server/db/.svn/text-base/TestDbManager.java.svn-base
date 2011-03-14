package org.wattdepot.server.db;

import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * Instantiates a DbManager and tests the database methods not tested elsewhere.
 * 
 * @author Robert Brewer
 */
public class TestDbManager extends DbManagerTestHelper {

  /**
   * Tests the performMaintenance method. All that really can be done is to call
   * performMaintenance() and expect it to return true.
   */
  @Test
  public void testPerformMaintenance() {
    assertTrue("Unable to perform DB maintenance", manager.performMaintenance());
  }

  /**
   * Tests the indexTables method. All that really can be done is to call indexTables() and expect
   * it to return true.
   */
  @Test
  public void testIndexTables() {
    assertTrue("Unable to index DB tables", manager.indexTables());
  }

  /**
   * Tests the makeSnapshot method. Note that at the DbManager level we cannot test any deeper than
   * this, since this could be a stub method for implementations that do not have snapshot support.
   * For a more in-depth test of snapshot creationg, see
   * org.wattdepot.server.db.derby.TestDerbyStorageImplementation().
   * 
   * @see org.wattdepot.server.db.derby.TestDerbyStorageImplementation
   */
  @Test
  public void testMakeSnapshot() {
    assertTrue("Unable to create snapshot", manager.makeSnapshot());
  }
}
