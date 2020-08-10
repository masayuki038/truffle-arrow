package net.wrap_trap.truffle_arrow;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.Comparator;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


public class GroupByWithFuncTest {
  @BeforeAll
  public static void setupOnce() throws ClassNotFoundException, IOException {
    Class.forName("net.wrap_trap.truffle_arrow.TruffleDriver");
    TestUtils.generateTestFile("target/classes/samples/files/all_fields.arrow", TestDataType.CASE5);
    TruffleArrowConfig.INSTANCE.reload();
  }

  @AfterAll
  public static void teardownOnce() {
    File file = new File("target/classes/samples/files/all_fields.arrow");
    if (!file.delete()) {
      throw new IllegalStateException("Failed to remove `all_fields.arrow`");
    }
  }

  @Test
  public void countByInt() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_INT, COUNT(F_INT) AS CNT from ALL_FIELDS group by F_INT");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(10));
      results.sort(Comparator.naturalOrder());
      assertThat(results.get(0), is("0\t1"));
      assertThat(results.get(1), is("1\t1"));
      assertThat(results.get(2), is("2\t1"));
      assertThat(results.get(3), is("3\t1"));
      assertThat(results.get(4), is("4\t1"));
      assertThat(results.get(5), is("5\t1"));
      assertThat(results.get(6), is("6\t1"));
      assertThat(results.get(7), is("7\t1"));
      assertThat(results.get(8), is("8\t1"));
      assertThat(results.get(9), is("9\t1"));
      assertThat(LastPlan.INSTANCE.includes(ArrowAggregate.class), is(true));
    }
  }

  @Test
  public void countByLong() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_BIGINT, COUNT(F_BIGINT) AS CNT from ALL_FIELDS group by F_BIGINT");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(5));
      results.sort(Comparator.naturalOrder());
      assertThat(results.get(0), is("0\t2"));
      assertThat(results.get(1), is("1\t2"));
      assertThat(results.get(2), is("2\t2"));
      assertThat(results.get(3), is("3\t2"));
      assertThat(results.get(4), is("4\t2"));
      assertThat(LastPlan.INSTANCE.includes(ArrowAggregate.class), is(true));
    }
  }

  @Test
  public void countByLong2() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select COUNT(F_BIGINT) AS CNT, F_BIGINT from ALL_FIELDS group by F_BIGINT");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(5));
      results.sort(Comparator.naturalOrder());
      assertThat(results.get(0), is("2\t0"));
      assertThat(results.get(1), is("2\t1"));
      assertThat(results.get(2), is("2\t2"));
      assertThat(results.get(3), is("2\t3"));
      assertThat(results.get(4), is("2\t4"));
      assertThat(LastPlan.INSTANCE.includes(ArrowAggregate.class), is(true));
    }
  }

  @Test
  public void countByVarchar() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_VARCHAR, COUNT(F_VARCHAR) AS CNT from ALL_FIELDS group by F_VARCHAR");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(5));
      results.sort(Comparator.naturalOrder());
      assertThat(results.get(0), is("test0\t2"));
      assertThat(results.get(1), is("test1\t2"));
      assertThat(results.get(2), is("test2\t2"));
      assertThat(results.get(3), is("test3\t2"));
      assertThat(results.get(4), is("test4\t2"));
      assertThat(LastPlan.INSTANCE.includes(ArrowAggregate.class), is(true));
    }
  }

  @Test
  public void countByTimestamp() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_TIMESTAMP, COUNT(F_TIMESTAMP) AS CNT from ALL_FIELDS group by F_TIMESTAMP");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(5));
      results.sort(Comparator.naturalOrder());
      assertThat(results.get(0), is("2020-05-04 13:48:11.0\t2"));
      assertThat(results.get(1), is("2020-05-05 13:48:11.0\t2"));
      assertThat(results.get(2), is("2020-05-06 13:48:11.0\t2"));
      assertThat(results.get(3), is("2020-05-07 13:48:11.0\t2"));
      assertThat(results.get(4), is("2020-05-08 13:48:11.0\t2"));
      assertThat(LastPlan.INSTANCE.includes(ArrowAggregate.class), is(true));
    }
  }

  @Test
  public void countByTime() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_TIME, COUNT(F_TIME) AS CNT from ALL_FIELDS group by F_TIME");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(5));
      results.sort(Comparator.naturalOrder());
      assertThat(results.get(0), is("01:20:23\t2"));
      assertThat(results.get(1), is("02:20:23\t2"));
      assertThat(results.get(2), is("03:20:23\t2"));
      assertThat(results.get(3), is("04:20:23\t2"));
      assertThat(results.get(4), is("05:20:23\t2"));
      assertThat(LastPlan.INSTANCE.includes(ArrowAggregate.class), is(true));
    }
  }

  @Test
  public void countByDate() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_DATE, COUNT(F_DATE) AS CNT from ALL_FIELDS group by F_DATE");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(5));
      results.sort(Comparator.naturalOrder());
      assertThat(results.get(0), is("2020-05-03\t2"));
      assertThat(results.get(1), is("2020-05-04\t2"));
      assertThat(results.get(2), is("2020-05-05\t2"));
      assertThat(results.get(3), is("2020-05-06\t2"));
      assertThat(results.get(4), is("2020-05-07\t2"));
      assertThat(LastPlan.INSTANCE.includes(ArrowAggregate.class), is(true));
    }
  }

  @Test
  public void countByDouble() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_DOUBLE, COUNT(F_DOUBLE) AS CNT from ALL_FIELDS group by F_DOUBLE");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(5));
      results.sort(Comparator.naturalOrder());
      assertThat(results.get(0), is("123.456\t2"));
      assertThat(results.get(1), is("124.456\t2"));
      assertThat(results.get(2), is("125.456\t2"));
      assertThat(results.get(3), is("126.456\t2"));
      assertThat(results.get(4), is("127.456\t2"));
      assertThat(LastPlan.INSTANCE.includes(ArrowAggregate.class), is(true));
    }
  }
}
