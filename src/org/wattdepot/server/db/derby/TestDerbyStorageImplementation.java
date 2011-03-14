package org.wattdepot.server.db.derby;

import static org.junit.Assert.assertTrue;
import java.io.File;
import java.util.Date;
import org.junit.Test;
import org.wattdepot.server.ServerProperties;
import org.wattdepot.server.db.DbManagerTestHelper;

/**
 * Tests functionality that is specific to the DerbyStorageImplementation, that cannot be assumed
 * at the DbManager level.
 * 
 * @author Robert Brewer
 */
public class TestDerbyStorageImplementation extends DbManagerTestHelper {
  /**
   * Tests the makeSnapshot method. Checks to see if the modification date of the snapshot dir is
   * updated by the method as a quick check that something happened.
   */
  @Test
  public void testMakeSnapshot() {
    File snapshotDir = new File(server.getServerProperties().get(ServerProperties.DB_SNAPSHOT_KEY));
    Date before = null;
    if (snapshotDir.exists()) {
      assertTrue("Snapshot path isn't a directory!", snapshotDir.isDirectory());
      before = new Date(snapshotDir.lastModified());
      // System.out.format("before: %1$ta %1$tb %1$td %1$tT.%1$tL %n", before); // DEBUG
    }
    else {
      System.out.println("before: [doesn't exist]");
    }
    assertTrue("Unable to create snapshot", manager.makeSnapshot());
    assertTrue("No snapshot directory created", snapshotDir.isDirectory());
    if (before != null) {
      // Only need to check if modification time changed if there was a snapshot directory before
      Date after = new Date(snapshotDir.lastModified());
      // System.out.format("after: %1$ta %1$tb %1$td %1$tT.%1$tL%n", after); // DEBUG
      assertTrue("Snapshot directory not modified", after.after(before));
    }
  }

}
