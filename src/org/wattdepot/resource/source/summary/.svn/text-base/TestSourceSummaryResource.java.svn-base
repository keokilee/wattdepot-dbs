package org.wattdepot.resource.source.summary;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;
import org.wattdepot.client.NotAuthorizedException;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.client.WattDepotClientException;
import org.wattdepot.resource.source.summary.jaxb.SourceSummary;
import org.wattdepot.test.ServerTestHelper;

/**
 * Tests the SourceSummary resource API at the HTTP level using WattDepotClient.
 * 
 * @author Robert Brewer
 */
public class TestSourceSummaryResource extends ServerTestHelper {

  private static final String PUBLIC_SUMMARY_NOT_FOUND = "Expected public SourceSummary not found";

  /** Holds SourceRefs of the default Sources for comparison in test cases. */
  private SourceSummary publicSourceSummary, privateSourceSummary;

  /**
   * Initializes variables used for tests.
   */
  @Before
  public void initializeVars() {
    this.publicSourceSummary = manager.getSourceSummary(defaultPublicSource);
    this.privateSourceSummary = manager.getSourceSummary(defaultPrivateSource);
  }

  // Tests for GET {host}/sources/{source}/summary

  /**
   * Tests retrieval of public SourceSummary. Type: no credentials.
   * 
   * @throws WattDepotClientException If problems are encountered
   */
  @Test
  public void testPublicSourceWithNoCredentials() throws WattDepotClientException {
    WattDepotClient client = new WattDepotClient(getHostName());
    assertEquals(PUBLIC_SUMMARY_NOT_FOUND, client.getSourceSummary(defaultPublicSource),
        this.publicSourceSummary);
  }

  /**
   * Tests retrieval of private SourceSummary. Type: no credentials.
   * 
   * @throws WattDepotClientException If problems are encountered
   */
  @Test(expected = NotAuthorizedException.class)
  public void testPrivateSourceWithNoCredentials() throws WattDepotClientException {
    WattDepotClient client = new WattDepotClient(getHostName());
    client.getSourceSummary(defaultPrivateSource);
    fail("Able to get private SourceSummary with no credentials");
  }

  /**
   * Tests retrieval of public SourceSummary. Type: invalid credentials.
   * 
   * @throws WattDepotClientException If problems are encountered
   */
  @Test(expected = NotAuthorizedException.class)
  public void testPublicSourceBadAuth() throws WattDepotClientException {
    // Shouldn't authenticate with invalid username or password
    WattDepotClient client = new WattDepotClient(getHostName(), adminEmail, "foo");
    client.getSourceSummary(defaultPublicSource);
    fail("Able to get SourceSummary with invalid credentials");
  }

  /**
   * Tests retrieval of private SourceSummary. Type: invalid credentials.
   * 
   * @throws WattDepotClientException If problems are encountered
   */
  @Test(expected = NotAuthorizedException.class)
  public void testPrivateSourceBadAuth() throws WattDepotClientException {
    // Shouldn't authenticate with invalid username or password
    WattDepotClient client = new WattDepotClient(getHostName(), adminEmail, "foo");
    client.getSourceSummary(defaultPrivateSource);
    fail("Able to get SourceSummary with invalid credentials");
  }

  /**
   * Tests retrieval of public & private SourceSummaries. Type: valid admin credentials.
   * 
   * @throws WattDepotClientException If problems are encountered
   */
  @Test
  public void testSourceWithAdminCredentials() throws WattDepotClientException {
    WattDepotClient client = new WattDepotClient(getHostName(), adminEmail, adminPassword);
    assertEquals(PUBLIC_SUMMARY_NOT_FOUND, client.getSourceSummary(defaultPublicSource),
        this.publicSourceSummary);
    assertEquals("Expected private source not found", client
        .getSourceSummary(defaultPrivateSource), this.privateSourceSummary);
  }

  /**
   * Tests retrieval of public & private SourceSummaries. Type: valid owner credentials.
   * 
   * @throws WattDepotClientException If problems are encountered
   */
  @Test
  public void testSourceWithOwnerCredentials() throws WattDepotClientException {
    WattDepotClient client =
        new WattDepotClient(getHostName(), defaultOwnerUsername,
            defaultOwnerPassword);
    assertEquals(PUBLIC_SUMMARY_NOT_FOUND, client.getSourceSummary(defaultPublicSource),
        this.publicSourceSummary);
    assertEquals("Expected private source not found", client
        .getSourceSummary(defaultPrivateSource), this.privateSourceSummary);
  }

  /**
   * Tests retrieval of public SourceSummary. Type: valid non-owner credentials.
   * 
   * @throws WattDepotClientException If problems are encountered
   */
  @Test
  public void testPublicSourceWithNonOwnerCredentials() throws WattDepotClientException {
    WattDepotClient client =
        new WattDepotClient(getHostName(), defaultNonOwnerUsername,
            defaultNonOwnerPassword);
    assertEquals(PUBLIC_SUMMARY_NOT_FOUND, client.getSourceSummary(defaultPublicSource),
        this.publicSourceSummary);
  }

  /**
   * Tests retrieval of private SourceSummary. Type: valid non-owner credentials.
   * 
   * @throws WattDepotClientException If problems are encountered
   */
  @Test(expected = NotAuthorizedException.class)
  public void testPrivateSourceWithNonOwnerCredentials() throws WattDepotClientException {
    WattDepotClient client =
        new WattDepotClient(getHostName(), defaultNonOwnerUsername,
            defaultNonOwnerPassword);
    client.getSourceSummary(defaultPrivateSource);
    fail("Able to get private SourceSummary with non-owner credentials");
  }
}
