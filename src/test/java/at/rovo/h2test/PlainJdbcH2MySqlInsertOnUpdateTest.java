package at.rovo.h2test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import org.h2.Driver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PlainJdbcH2MySqlInsertOnUpdateTest {

  private Connection dbConnection = null;

  @Before
  public void initDB() throws Exception {
    Properties props = new Properties();
    props.setProperty("DB_CLOSE_DELAY", "1");
    props.setProperty("MODE", "MYSQL");
    this.dbConnection =
        Driver.load().connect("jdbc:h2:mem:testdb;", props);

    execute("CREATE TABLE message ("
        + "id bigint(20) NOT NULL AUTO_INCREMENT, "
        + "messageId varchar(255) DEFAULT NULL, "
        + "message longblob, "
        + "lastStatusChange dateTime DEFAULT NULL, "
        + "PRIMARY KEY (id), "
        + "UNIQUE KEY UK_msgId (messageId), "
        + "KEY idx_lastStatusChange (lastStatusChange) "
        + ") ENGINE=InnoDB AUTO_INCREMENT=50 DEFAULT CHARSET=UTF8");

    execute("CREATE TABLE status ("
        + "id bigint(20) NOT NULL AUTO_INCREMENT, "
        + "lastChange datetime DEFAULT NULL, "
        + "messageId bigint(20) DEFAULT NULL,"
        + "status varchar(255) DEFAULT NULL, "
        + "PRIMARY KEY (id), "
        + "KEY idx_lastChange (lastChange), "
        + "KEY fk_status_message (messageId), "
        + "CONSTRAINT fk_status_message FOREIGN KEY (messageId) REFERENCES message (id) "
        + ") ENGINE=InnoDB AUTO_INCREMENT=84 DEFAULT CHARSET=UTF8");

    execute("INSERT INTO message (messageId, message, lastStatusChange) VALUES ('abcd1234', RAWTOHEX('Test Message 1'), '2015-09-21 10:34:09')");
    execute("INSERT INTO status (lastChange, messageId, status) VALUES ('2015-09-21 10:34:09', 1, 'RECEIVED')");
    execute("INSERT INTO status (lastChange, messageId, status) VALUES ('2015-09-21 10:34:09', 1, 'DELIVERED')");
    execute("INSERT INTO message (messageId, message, lastStatusChange) VALUES ('abcd1235', RAWTOHEX('Test Message 2'), '2015-09-21 10:34:09')");
    execute("INSERT INTO status (lastChange, messageId, status) VALUES ('2015-09-21 10:34:09', 2, 'RECEIVED')");
    execute("INSERT INTO status (lastChange, messageId, status) VALUES ('2015-09-21 10:34:09', 2, 'DELIVERED')");
    execute("INSERT INTO message (messageId, message, lastStatusChange) VALUES ('abcd1236', RAWTOHEX('Test Message 3'), '2015-09-21 10:34:09')");
    execute("INSERT INTO status (lastChange, messageId, status) VALUES ('2015-09-21 10:34:09', 3, 'RECEIVED')");
    execute("INSERT INTO message (messageId, message, lastStatusChange) VALUES ('abcd1237', RAWTOHEX('Test Message 4'), '2015-09-21 10:34:09')");
    execute("INSERT INTO status (lastChange, messageId, status) VALUES ('2015-09-21 10:34:09', 4, 'RECEIVED')");
  }

  @Test
  public void insertOnUpdateTestWithForeignKey() throws Exception {

    PreparedStatement insetUpdate = null;
    PreparedStatement insert = null;
    try {
      dbConnection.setAutoCommit(false);
      dbConnection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);

      insetUpdate = dbConnection.prepareStatement("INSERT INTO message (messageId, message, lastStatusChange) VALUES ('abcd1234', RAWTOHEX('Updated Message 1'), '2015-09-21 10:40:00') ON DUPLICATE KEY UPDATE message=VALUES(RAWTOHEX('Updated Message 1')), lastStatusChange=VALUES('2015-09-21 10:40:00')");
      int retValue = insetUpdate.executeUpdate();

      Statement s = dbConnection.createStatement();
      ResultSet rs = s.executeQuery("SELECT LAST_INSERT_ID() AS n");
      rs.next();
      int affectedId = rs.getInt(1);
      if (1 == retValue) {
        System.out.println("Inserted new entry with ID: " + affectedId);
        // fail("An update should have been triggered instead of an insert!");
      } else {
        System.out.println("Updated entry with ID: " + affectedId);
      }

      insert = dbConnection.prepareStatement("INSERT INTO status (lastChange, messageId, status) VALUES ('2015-09-21 10:40:00', " + affectedId + ", 'UPDATED')");
      insert.execute();

      dbConnection.commit();
    } catch (Exception ex) {
      System.err.println("Caught exception while performing transaction. Reason: " + ex.getLocalizedMessage());
      dbConnection.rollback();
    } finally {
      if (null != insetUpdate) {
        insetUpdate.close();
      }
      if (null != insert) {
        insert.close();
      }
    }

    Statement s = dbConnection.createStatement();
    ResultSet rs = s.executeQuery("SELECT count(*) FROM message");
    rs.next();
    assertThat(rs.getInt(1), is(equalTo(4)));
    rs.close();

    rs = s.executeQuery("SELECT count(*) FROM status WHERE messageId = 1");
    rs.next();
    assertThat(rs.getInt(1), is(equalTo(3)));
  }

  @After
  public void close() throws Exception {
    dbConnection.close();
  }

  private void execute(String statement) throws Exception {
    try (PreparedStatement stmt = dbConnection.prepareStatement(statement)) {
      stmt.execute();
    }
  }
}
