package org.wattdepot.resource.db;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.wattdepot.client.NotAuthorizedException;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.client.WattDepotClientException;
import org.wattdepot.test.ServerTestHelper;

/**
 * Tests the Database resource at the HTTP level using WattDepotClient.
 * 
 * @author Robert Brewer
 */
public class TestDatabaseResource extends ServerTestHelper {

  /**
   * Tests creation of snapshot. Type: no credentials.
   * 
   * @throws WattDepotClientException If problems are encountered
   */
  @Test(expected = NotAuthorizedException.class)
  public void testSnapshotNoCredentials() throws WattDepotClientException {
    WattDepotClient client = new WattDepotClient(getHostName());
    assertFalse("Able to create snapshot with no credentials", client.makeSnapshot());
  }

  /**
   * Tests creation of snapshot. Type: valid owner credentials.
   * 
   * @throws WattDepotClientException If problems are encountered
   */
  @Test(expected = NotAuthorizedException.class)
  public void testSnapshotOwnerCredentials() throws WattDepotClientException {
    WattDepotClient client =
        new WattDepotClient(getHostName(), defaultOwnerUsername, defaultOwnerPassword);
    assertFalse("Able to create snapshot with owner credentials", client.makeSnapshot());
  }

  /**
   * Tests creation of snapshot. Type: admin credentials.
   * 
   * @throws WattDepotClientException If problems are encountered
   */
  @Test
  public void testSnapshot() throws WattDepotClientException {
    WattDepotClient client = new WattDepotClient(getHostName(), adminEmail, adminPassword);
    assertTrue("Able to create snapshot with owner credentials", client.makeSnapshot());
  }
}
