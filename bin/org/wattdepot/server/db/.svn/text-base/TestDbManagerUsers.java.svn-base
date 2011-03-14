package org.wattdepot.server.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.wattdepot.server.ServerProperties.ADMIN_EMAIL_KEY;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.wattdepot.resource.user.jaxb.User;
import org.wattdepot.resource.user.jaxb.UserRef;

/**
 * Instantiates a DbManager and tests the database methods related to User resources.
 * 
 * @author Robert Brewer
 */
public class TestDbManagerUsers extends DbManagerTestHelper {

  /** Test users used by the tests, but never changed. */
  private final User user1 = makeTestUser1(), user2 = makeTestUser2();

  /** Make PMD happy. */
  private static final String USER_DOES_NOT_MATCH =
      "Retrieved user does not match original stored user";

  /**
   * Tests the getUsers method.
   */
  @Test
  public void testGetUsers() {
    String adminUsername = server.getServerProperties().get(ADMIN_EMAIL_KEY);
    User adminUser = manager.getUser(adminUsername);
    UserRef adminUserRef = new UserRef(adminUser, server);
    
    // case #1: Database starts fresh with just admin user.
    assertEquals("Freshly created database contains unexpected users", manager.getUsers()
        .getUserRef().get(0), adminUserRef);

    // case #2: after storing a single User, should have UserIndex with one UserRef that matches
    // User
    assertTrue("Unable to store user1 in DB", manager.storeUser(this.user1));
    // Confirm that getUsers returns a single UserRef now
    assertSame("getUsers returned wrong number of UserRefs",
        manager.getUsers().getUserRef().size(), 2);
    // Confirm that the second UserRef is from user1
    assertTrue("getUsers didn't return expected UserRef", manager.getUsers().getUserRef().get(1)
        .equalsUser(this.user1));

    // case #3: after storing two Users, should have UserIndex with two UserRefs that match Users
    assertTrue("Unable to store user2 in DB", manager.storeUser(this.user2));
    // Confirm that getUsers returns two UserRefs now
    assertSame("getUsers returned wrong number of UserRefs",
        manager.getUsers().getUserRef().size(), 3);
    // Confirm that the two UserRefs are equivalent to user1 and user2
    List<UserRef> refs = manager.getUsers().getUserRef();
    List<User> origUsers = new ArrayList<User>();
    origUsers.add(this.user1);
    origUsers.add(this.user2);
    origUsers.add(adminUser);
    for (UserRef ref : refs) {
      int found = 0;
      for (User user : origUsers) {
        if (ref.equalsUser(user)) {
          found++;
        }
      }
      assertSame("UserRefs from getUsers do not match input Users", found, 1);
    }
    // Confirm that user list is sorted
    // clear list
    origUsers.clear();
    // user1's name comes before user2, so add in opposite order
    origUsers.add(adminUser);
    origUsers.add(this.user2);
    origUsers.add(this.user1);
    for (int i = 0; i < origUsers.size(); i++) {
      assertTrue("getUsers index not sorted", refs.get(i).equalsUser(origUsers.get(i)));
    }

    // case #4: after deleting a User should have single UserRef in UserIndex.
    assertTrue("Unable to delete user1", manager.deleteUser(this.user1.getEmail()));
    assertSame("getUsers returned wrong number of UserRefs",
        manager.getUsers().getUserRef().size(), 2);
    // Confirm that the single UserRef is from user2
    assertTrue("getUsers didn't return expected UserRef", manager.getUsers().getUserRef().get(1)
        .equalsUser(this.user2));
  }

  /**
   * Tests the getUser method.
   */
  @Test
  public void testGetUser() {
    // Test cases: retrieve user from empty DB, retrieve stored user, retrieve unknown username,
    // store 2 users and verify retrieval, retrieve empty username, retrieve null username.

    // case #1: retrieve user from empty DB
    assertNull("Able to retrieve user from empty DB", manager.getUser(this.user1.getEmail()));

    // case #2: store user, retrieve stored user and compare to original user
    assertTrue("Unable to store a User in DB", manager.storeUser(this.user1));
    assertEquals(USER_DOES_NOT_MATCH, this.user1, manager.getUser(this.user1.getEmail()));

    // case #3: retrieve unknown username
    assertNull("Able to retrieve User with ficticious username", manager
        .getUser("boguslongusername@example.com"));

    // case #4: store second User and verify retrieval of both Users
    assertTrue("Unable to store user2 in DB", manager.storeUser(this.user2));
    assertEquals(USER_DOES_NOT_MATCH, user2, manager.getUser(this.user2.getEmail()));
    assertEquals(USER_DOES_NOT_MATCH, this.user1, manager.getUser(this.user1.getEmail()));

    // case #5: retrieve empty username
    assertNull("Able to retrieve User with empty username", manager.getUser(""));

    // case #5: retrieve null username
    assertNull("Able to retrieve User with null username", manager.getUser(null));
  }

  /**
   * Tests the storeUser method.
   */
  @Test
  public void testStoreUser() {
    // Test cases: store and retrieve user, attempt to overwrite existing user,
    // store 2 users and verify retrieval, store null User.

    // case #1: store user, retrieve stored user and compare to original user
    assertTrue("Unable to store a User in DB", manager.storeUser(this.user1));
    assertEquals(USER_DOES_NOT_MATCH, this.user1, manager.getUser(this.user1.getEmail()));

    // case #2: attempt to overwrite existing user
    assertFalse("Overwriting user succeeded, but should fail", manager.storeUser(this.user1));

    // case #3: store second User and verify retrieval of both Users
    assertTrue("Unable to store user2 in DB", manager.storeUser(this.user2));
    assertEquals(USER_DOES_NOT_MATCH, this.user2, manager.getUser(this.user2.getEmail()));
    assertEquals(USER_DOES_NOT_MATCH, this.user1, manager.getUser(this.user1.getEmail()));

    // case #4: store null User
    assertFalse("Storing null user succeeded, but should fail", manager.storeUser(null));
  }

  /**
   * Tests the deleteUser method.
   */
  @Test
  public void testDeleteUser() {
    // Test cases: delete user from empty database, delete stored user, delete deleted user,
    // delete unknown username, delete empty username, delete null username.

    // case #1: delete user from empty database
    assertFalse("Able to delete user1 from empty database", manager.deleteUser(this.user1
        .getEmail()));

    // case #2: store user, then delete user and confirm deletion
    assertTrue("Unable to store a User in DB", manager.storeUser(this.user1));
    assertEquals(USER_DOES_NOT_MATCH, this.user1, manager.getUser(this.user1.getEmail()));
    assertTrue("Unable to delete user1", manager.deleteUser(this.user1.getEmail()));
    assertNull("Able to retrieve deleted user", manager.getUser(this.user1.getEmail()));

    // case #3: delete deleted user
    assertFalse("Able to delete user1 a second time", manager.deleteUser(this.user1.getEmail()));

    // case #4: delete unknown username
    assertFalse("Able to delete non-existent user", manager
        .deleteUser("boguslongusername@example.com"));

    // case #5: delete empty username
    assertFalse("Able to delete empty username", manager.deleteUser(""));

    // case #6: delete null username
    assertFalse("Able to delete empty username", manager.deleteUser(null));

    // TODO add case to check that Sources (& sensor data) owned by User are deleted when User is
    // TODO deleted
  }
}
