package net.wrap_trap.truffle_arrow;

import java.sql.*;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


/**
 * Query tests for ArrowTable
 */
public class JdbcTest {

  @BeforeAll
  public static void setupOnce() throws ClassNotFoundException {
    Class.forName("net.wrap_trap.truffle_arrow.TruffleDriver");
    TruffleArrowConfig.INSTANCE.reload();
  }

  @Test
  public void metadata() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select N_NATIONKEY, N_NAME, N_REGIONKEY from NATIONSSF");
      ResultSet rs = pstmt.executeQuery()
    ) {
      ResultSetMetaData meta = rs.getMetaData();
      int count = meta.getColumnCount();
      assertThat(count, is(3));
      assertThat(meta.getColumnName(1), is("N_NATIONKEY"));
      assertThat(meta.getColumnType(1), is(Types.BIGINT));
      assertThat(meta.getColumnName(2), is("N_NAME"));
      assertThat(meta.getColumnType(2), is(Types.VARCHAR));
      assertThat(meta.getColumnName(3), is("N_REGIONKEY"));
      assertThat(meta.getColumnType(3), is(Types.BIGINT));
    }
  }

  @Test
  public void simpleProjection() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select N_NATIONKEY, N_NAME, N_REGIONKEY from NATIONSSF");
      ResultSet rs = pstmt.executeQuery()
        ) {
      List<String> results = TestUtils.getResults(rs);
      System.out.println(results);
      assertThat(results.size(), is(25));
      assertThat(results.get(0), is("0\tALGERIA\t0"));
      assertThat(results.get(24), is("24\tUNITED STATES\t1"));
    }
  }

  @Test
  public void testStatementExecuteQuery() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      ResultSet rs = conn.createStatement().executeQuery(
        "select N_NATIONKEY, N_NAME, N_REGIONKEY from NATIONSSF");
    ) {
      List<String> results = TestUtils.getResults(rs);
      System.out.println(results);
      assertThat(results.size(), is(25));
      assertThat(results.get(0), is("0\tALGERIA\t0"));
      assertThat(results.get(24), is("24\tUNITED STATES\t1"));
    }
  }
}
