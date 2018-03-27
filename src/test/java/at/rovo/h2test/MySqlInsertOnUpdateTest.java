package at.rovo.h2test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import javax.annotation.Resource;
import javax.sql.DataSource;
import org.h2.jdbc.JdbcSQLException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;

@SuppressWarnings({"SqlNoDataSourceInspection", "SqlDialectInspection"})
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes= {MySqlInsertOnUpdateTest.ContextConfig.class})
public class MySqlInsertOnUpdateTest {

  @Resource
  private JdbcTemplate jdbcTemplate;
  @Resource
  private PlatformTransactionManager tm;

  private void initDB() {

    jdbcTemplate.execute("DROP FUNCTION IF EXISTS RAWTOHEX");
    jdbcTemplate.execute("CREATE FUNCTION RAWTOHEX(message VARCHAR(64)) RETURNS VARCHAR(64) RETURN HEX(message)");

    jdbcTemplate.execute("DROP TABLE IF EXISTS status");
    jdbcTemplate.execute("DROP TABLE IF EXISTS message");

    jdbcTemplate.execute("CREATE TABLE message ("
        + "id bigint(20) NOT NULL AUTO_INCREMENT, "
        + "messageId varchar(255) DEFAULT NULL, "
        + "message longblob, "
        + "lastStatusChange dateTime DEFAULT NULL, "
        + "PRIMARY KEY (id), "
        + "UNIQUE KEY UK_msgId (messageId), "
        + "KEY idx_lastStatusChange (lastStatusChange) "
        + ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=UTF8");

    jdbcTemplate.execute("CREATE TABLE status ("
        + "id bigint(20) NOT NULL AUTO_INCREMENT, "
        + "lastChange datetime DEFAULT NULL, "
        + "messageId bigint(20) DEFAULT NULL,"
        + "status varchar(255) DEFAULT NULL, "
        + "PRIMARY KEY (id), "
        + "KEY idx_lastChange (lastChange), "
        + "KEY fk_status_message (messageId), "
        + "CONSTRAINT fk_status_message FOREIGN KEY (messageId) REFERENCES message (id) "
        + ") ENGINE=InnoDB AUTO_INCREMENT=84 DEFAULT CHARSET=UTF8");

    jdbcTemplate.execute("INSERT INTO message (messageId, message, lastStatusChange) VALUES ('abcd1234', RAWTOHEX('Test Message 1'), '2015-09-21 10:34:09')");
    jdbcTemplate.execute("INSERT INTO status (lastChange, messageId, status) VALUES ('2015-09-21 10:34:09', 1, 'RECEIVED')");
    jdbcTemplate.execute("INSERT INTO status (lastChange, messageId, status) VALUES ('2015-09-21 10:34:09', 1, 'DELIVERED')");
    jdbcTemplate.execute("INSERT INTO message (messageId, message, lastStatusChange) VALUES ('abcd1235', RAWTOHEX('Test Message 2'), '2015-09-21 10:34:09')");
    jdbcTemplate.execute("INSERT INTO status (lastChange, messageId, status) VALUES ('2015-09-21 10:34:09', 2, 'RECEIVED')");
    jdbcTemplate.execute("INSERT INTO status (lastChange, messageId, status) VALUES ('2015-09-21 10:34:09', 2, 'DELIVERED')");
    jdbcTemplate.execute("INSERT INTO message (messageId, message, lastStatusChange) VALUES ('abcd1236', RAWTOHEX('Test Message 3'), '2015-09-21 10:34:09')");
    jdbcTemplate.execute("INSERT INTO status (lastChange, messageId, status) VALUES ('2015-09-21 10:34:09', 3, 'RECEIVED')");
    jdbcTemplate.execute("INSERT INTO message (messageId, message, lastStatusChange) VALUES ('abcd1237', RAWTOHEX('Test Message 4'), '2015-09-21 10:34:09')");
    jdbcTemplate.execute("INSERT INTO status (lastChange, messageId, status) VALUES ('2015-09-21 10:34:09', 4, 'RECEIVED')");
  }

  @Test
  public void insertOnUpdateTestWithForeignKey() throws Exception {
    initDB();

    int numMessages = jdbcTemplate.queryForObject("SELECT count(*) FROM message", Integer.class);
    assertThat("Unexpected number of messages found after initialization", numMessages, is(equalTo(4)));

    TransactionTemplate txTemp = new TransactionTemplate(tm);
    txTemp.setIsolationLevel(TransactionDefinition.ISOLATION_READ_UNCOMMITTED);
    txTemp.execute(new TransactionCallbackWithoutResult() {
      @Override
      protected void doInTransactionWithoutResult(TransactionStatus status) {

        int testNumMessages;

        // This query will actually increase the ID returned by later INSERT INTO ... ON DUPLICATE KEY UPDATE statement by 1!
        try {
          final String sqlInsert = "INSERT INTO message (messageId, message, lastStatusChange) VALUES ('abcd1234', RAWTOHEX('Updated Message 1'), '2015-09-21 10:40:00')";
          jdbcTemplate.update(sqlInsert);
          fail("Should have thrown an exception as entry already exists");
        } catch (DataAccessException daEx) {
          assertThat(daEx.getCause(), instanceOf(MySQLIntegrityConstraintViolationException.class));
          // Unique index or primary key violated: "UK_MSGID_INDEX_6 ON PUBLIC.MESSAGE(MESSAGEID) VALUES ('abcd1234', 1)";
          //    SQL statement: INSERT INTO message (messageId, message, lastStatusChange)
          //                   VALUES ('abcd1234', RAWTOHEX('Updated Message 1'), '2015-09-21 10:40:00') [23505-197]
          testNumMessages = jdbcTemplate.queryForObject("SELECT count(*) FROM message", Integer.class);
          assertThat("Unexpected number of messages after first insert test found", testNumMessages, is(equalTo(4)));
        }



        // ... This statement however does not!
        try {
          final String sqlInsert = "INSERT INTO message (id, messageId, message, lastStatusChange) VALUES (1, 'abcd1234', RAWTOHEX('Updated Message 1'), '2015-09-21 10:40:00')";
          jdbcTemplate.update(sqlInsert);
          fail("Should have thrown an exception as entry already exists");
        } catch (DataAccessException daEx) {
          assertThat(daEx.getCause(), instanceOf(MySQLIntegrityConstraintViolationException.class));
          // Unique index or primary key violated: "PRIMARY KEY ON PUBLIC.MESSAGE(ID)" Unique index or primary key violation: "PRIMARY KEY ON PUBLIC.MESSAGE(ID)";
          //    SQL statement: INSERT INTO message (id, messageId, message, lastStatusChange)
          //                   VALUES (1, 'abcd1234', RAWTOHEX('Updated Message 1'), '2015-09-21 10:40:00') [23505-197]
          testNumMessages = jdbcTemplate.queryForObject("SELECT count(*) FROM message", Integer.class);
          assertThat("Unexpected number of messages after insert with ID test found", testNumMessages, is(equalTo(4)));
        }


        // This simple transaction should update the first entry and return the index of the updated row.
        // However, the statement returns the next available row index which leads to the following failure:
        // Referential integrity constraint violation: "FK_STATUS_MESSAGE: PUBLIC.STATUS FOREIGN KEY(MESSAGEID) REFERENCES PUBLIC.MESSAGE(ID) (5)"
        // when the second SQL statement, which has a foreign key to the primer one, is executed !

        KeyHolder keyHolder = new GeneratedKeyHolder();
        final String sqlInsertUpdate = "INSERT INTO message (messageId, message, lastStatusChange) VALUES ('abcd1234', RAWTOHEX('Updated Message 1'), '2015-09-21 10:40:00') ON DUPLICATE KEY UPDATE message=RAWTOHEX('Updated Message 1'), lastStatusChange='2015-09-21 10:40:00'";
        int numMsg =
            jdbcTemplate.update(new PreparedStatementCreator() {
              @Override
              public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
                return con.prepareStatement(sqlInsertUpdate, new String[] { "id" });
              }
            }, keyHolder);

        System.out.println("Affected rows: " + numMsg);

        Long messageRefId = null;
        if (keyHolder.getKeyList().size() > 1) {
          if (keyHolder.getKeyList().get(0).containsKey("GENERATED_KEY")) {
            messageRefId = (Long) keyHolder.getKeyList().get(0).get("GENERATED_KEY");
          } else if (keyHolder.getKeyList().get(0).containsKey("ID")) {
            messageRefId = (Long) keyHolder.getKeyList().get(0).get("ID");
          }
        } else {
          Map<String, Object> keys = keyHolder.getKeys();
          if (keys.containsKey("GENERATED_KEY")) {
            messageRefId = (Long) keys.get("GENERATED_KEY");
          } else if (keys.containsKey("ID")) {
            messageRefId = (Long) keys.get("ID");
          }
        }

        assertThat("Invalid ID of referenced table entry returned", messageRefId, is(equalTo(1L)));

        String sqlInsert = "INSERT INTO status (lastChange, messageId, status) VALUES ('2015-09-21 10:40:00', " + messageRefId + ", 'UPDATED')";
        numMsg = jdbcTemplate.update(sqlInsert);

        System.out.println("Affected rows: " + numMsg);
      }
    });

    numMessages = jdbcTemplate.queryForObject("SELECT count(*) FROM message", Integer.class);
    assertThat("Unexpected number of stored messages after test found", numMessages, is(equalTo(4)));
    numMessages = jdbcTemplate.queryForObject("SELECT count(*) FROM status WHERE messageId = 1", Integer.class);
    assertThat("Unexpected number of states for first message found after tests", numMessages, is(equalTo(3)));
  }

  @Configuration
  public static class ContextConfig {

    @Bean
    public DataSource dataSource() throws Exception {
      SimpleDriverDataSource db = new SimpleDriverDataSource();
      db.setDriverClass(com.mysql.jdbc.Driver.class);
      db.setUrl("jdbc:mysql://localhost:3306/test?useSSL=false");
      db.setUsername("root");
      return db;
    }

    @Bean
    public JdbcTemplate jdbcTemplate() throws Exception {
      return new JdbcTemplate(dataSource());
    }

    @Bean
    public PlatformTransactionManager transactionManager() throws Exception {
      return new DataSourceTransactionManager(dataSource());
    }
  }
}

