package net.wrap_trap.truffle_arrow;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.*;
import java.util.Comparator;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class AggregateSumTest {
  @BeforeAll
  public static void setupOnce() throws ClassNotFoundException, IOException {
    Class.forName("net.wrap_trap.truffle_arrow.TruffleDriver");
    TestUtils.generateTestFiles("target/classes/samples/files/all_fields", TestDataType.CASE5);
    TruffleArrowConfig.INSTANCE.reload();
  }

  @AfterAll
  public static void teardownOnce() throws IOException {
    TestUtils.deleteDirectory("target/classes/samples/files/all_fields");
  }

  @Test
  public void sumByInt() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_INT, SUM(F_INT) AS CNT from ALL_FIELDS group by F_INT");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(10));
      results.sort(Comparator.naturalOrder());
      assertThat(results.get(0), is("0\t0"));
      assertThat(results.get(1), is("1\t1"));
      assertThat(results.get(2), is("2\t2"));
      assertThat(results.get(3), is("3\t3"));
      assertThat(results.get(4), is("4\t4"));
      assertThat(results.get(5), is("5\t5"));
      assertThat(results.get(6), is("6\t6"));
      assertThat(results.get(7), is("7\t7"));
      assertThat(results.get(8), is("8\t8"));
      assertThat(results.get(9), is("9\t9"));
      assertThat(LastPlan.INSTANCE.includes(ArrowAggregate.class), is(true));
    }
  }

  @Test
  public void sumByLong() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_BIGINT, SUM(F_INT) AS CNT from ALL_FIELDS group by F_BIGINT");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(5));
      results.sort(Comparator.naturalOrder());
      assertThat(results.get(0), is("0\t5"));
      assertThat(results.get(1), is("1\t7"));
      assertThat(results.get(2), is("2\t9"));
      assertThat(results.get(3), is("3\t11"));
      assertThat(results.get(4), is("4\t13"));
      assertThat(LastPlan.INSTANCE.includes(ArrowAggregate.class), is(true));
    }
  }

  @Test
  public void sumByLong2() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select SUM(F_INT), F_BIGINT AS CNT from ALL_FIELDS group by F_BIGINT");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(5));
      results.sort(Comparator.naturalOrder());
      assertThat(results.get(0), is("11\t3"));
      assertThat(results.get(1), is("13\t4"));
      assertThat(results.get(2), is("5\t0"));
      assertThat(results.get(3), is("7\t1"));
      assertThat(results.get(4), is("9\t2"));
      assertThat(LastPlan.INSTANCE.includes(ArrowAggregate.class), is(true));
    }
  }

  @Test
  public void sumByVarchar() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_VARCHAR, SUM(F_INT) AS CNT from ALL_FIELDS group by F_VARCHAR");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(5));
      results.sort(Comparator.naturalOrder());
      assertThat(results.get(0), is("test0\t5"));
      assertThat(results.get(1), is("test1\t7"));
      assertThat(results.get(2), is("test2\t9"));
      assertThat(results.get(3), is("test3\t11"));
      assertThat(results.get(4), is("test4\t13"));
      assertThat(LastPlan.INSTANCE.includes(ArrowAggregate.class), is(true));
    }
  }

  @Test
  public void sumByTimestamp() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_TIMESTAMP, SUM(F_BIGINT) AS CNT from ALL_FIELDS group by F_TIMESTAMP");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(5));
      results.sort(Comparator.naturalOrder());
      assertThat(results.get(0), is("2020-05-04 13:48:11.0\t0"));
      assertThat(results.get(1), is("2020-05-05 13:48:11.0\t2"));
      assertThat(results.get(2), is("2020-05-06 13:48:11.0\t4"));
      assertThat(results.get(3), is("2020-05-07 13:48:11.0\t6"));
      assertThat(results.get(4), is("2020-05-08 13:48:11.0\t8"));
      assertThat(LastPlan.INSTANCE.includes(ArrowAggregate.class), is(true));
    }
  }

  @Test
  public void sumByTime() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_TIME, SUM(F_INT) AS CNT from ALL_FIELDS group by F_TIME");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(5));
      results.sort(Comparator.naturalOrder());
      assertThat(results.get(0), is("01:20:23\t5"));
      assertThat(results.get(1), is("02:20:23\t7"));
      assertThat(results.get(2), is("03:20:23\t9"));
      assertThat(results.get(3), is("04:20:23\t11"));
      assertThat(results.get(4), is("05:20:23\t13"));
      assertThat(LastPlan.INSTANCE.includes(ArrowAggregate.class), is(true));
    }
  }

  @Test
  public void sumByDate() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_DATE, SUM(F_BIGINT) AS CNT from ALL_FIELDS group by F_DATE");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(5));
      results.sort(Comparator.naturalOrder());
      assertThat(results.get(0), is("2020-05-03\t0"));
      assertThat(results.get(1), is("2020-05-04\t2"));
      assertThat(results.get(2), is("2020-05-05\t4"));
      assertThat(results.get(3), is("2020-05-06\t6"));
      assertThat(results.get(4), is("2020-05-07\t8"));
      assertThat(LastPlan.INSTANCE.includes(ArrowAggregate.class), is(true));
    }
  }

  @Test
  public void sumByDouble() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_DOUBLE, SUM(F_DOUBLE) AS CNT from ALL_FIELDS group by F_DOUBLE");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(5));
      results.sort(Comparator.naturalOrder());
      assertThat(results.get(0), is("123.456\t246.912"));
      assertThat(results.get(1), is("124.456\t248.912"));
      assertThat(results.get(2), is("125.456\t250.912"));
      assertThat(results.get(3), is("126.456\t252.912"));
      assertThat(results.get(4), is("127.456\t254.912"));
      assertThat(LastPlan.INSTANCE.includes(ArrowAggregate.class), is(true));
    }
  }

  @Test
  public void sumByLongAndVarchar() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_BIGINT, F_VARCHAR, SUM(F_INT) AS CNT from ALL_FIELDS group by F_BIGINT, F_VARCHAR");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(5));
      results.sort(Comparator.naturalOrder());
      assertThat(results.get(0), is("0\ttest0\t5"));
      assertThat(results.get(1), is("1\ttest1\t7"));
      assertThat(results.get(2), is("2\ttest2\t9"));
      assertThat(results.get(3), is("3\ttest3\t11"));
      assertThat(results.get(4), is("4\ttest4\t13"));
      assertThat(LastPlan.INSTANCE.includes(ArrowAggregate.class), is(true));
    }
  }

  @Test
  public void sumByVarcharAndLong() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select SUM(F_INT) AS CNT, F_BIGINT, F_VARCHAR from ALL_FIELDS group by F_VARCHAR, F_BIGINT");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(5));
      results.sort(Comparator.naturalOrder());
      assertThat(results.get(0), is("11\t3\ttest3"));
      assertThat(results.get(1), is("13\t4\ttest4"));
      assertThat(results.get(2), is("5\t0\ttest0"));
      assertThat(results.get(3), is("7\t1\ttest1"));
      assertThat(results.get(4), is("9\t2\ttest2"));
      assertThat(LastPlan.INSTANCE.includes(ArrowAggregate.class), is(true));
    }
  }
}
