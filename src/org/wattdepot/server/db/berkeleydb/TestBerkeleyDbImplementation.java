package org.wattdepot.server.db.berkeleydb;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.io.File;
import org.junit.Test;
import org.wattdepot.server.db.DbManagerTestHelper;

/**
 * Tests specifically for the BerkeleyDbImplementation.
 * 
 * @author George Lee
 *
 */
public class TestBerkeleyDbImplementation extends DbManagerTestHelper {
  
  /**
   * Tests the makeSnapshot method. We check to see if the files are moved over from the berkeleydb folder.
   */
  @Test
  public void testMakeSnapshot() {
    File backupDir = new File("berkeleydb", "backup");
    assertNotNull("Backup folder is not available", backupDir);
    assertTrue("Unable to create snapshot", manager.makeSnapshot());
    File[] files = backupDir.listFiles();
    boolean found = false;
    for (File file : files) {
      if (file.getName().endsWith(".jdb")) {
        found = true;
      }
    }
    assertTrue("Could not find any backed up files.", found);
  }
}
