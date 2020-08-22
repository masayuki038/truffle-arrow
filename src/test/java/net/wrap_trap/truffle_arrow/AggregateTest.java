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

public class AggregateTest {

  @BeforeAll
  public static void setupOnce() throws ClassNotFoundException, IOException {
    Class.forName("net.wrap_trap.truffle_arrow.TruffleDriver");
    TestUtils.generateTestFile("target/classes/samples/files/all_fields.arrow", TestDataType.CASE5);
    TruffleArrowConfig.INSTANCE.reload();
  }

  @AfterAll
  public static void teardownOnce() {
    new File("target/classes/samples/files/all_fields.arrow").delete();
  }

  @Test
  public void groupByInt() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_INT from ALL_FIELDS group by F_INT");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(10));
      results.sort(Comparator.naturalOrder());
      assertThat(results.get(0), is("0"));
      assertThat(LastPlan.INSTANCE.includes(ArrowAggregate.class), is(true));
    }
  }

  @Test
  public void groupByLong() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_BIGINT from ALL_FIELDS group by F_BIGINT");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(5));
      results.sort(Comparator.naturalOrder());
      assertThat(results.get(0), is("0"));
      assertThat(LastPlan.INSTANCE.includes(ArrowAggregate.class), is(true));
    }
  }

  @Test
  public void groupByVarchar() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_VARCHAR from ALL_FIELDS group by F_VARCHAR");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(5));
      results.sort(Comparator.naturalOrder());
      assertThat(results.get(0), is("test0"));
      assertThat(LastPlan.INSTANCE.includes(ArrowAggregate.class), is(true));
    }
  }

  @Test
  public void groupByTimestamp() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_TIMESTAMP from ALL_FIELDS group by F_TIMESTAMP");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(5));
      results.sort(Comparator.naturalOrder());
      assertThat(results.get(0), is("2020-05-04 13:48:11.0"));
      assertThat(LastPlan.INSTANCE.includes(ArrowAggregate.class), is(true));
    }
  }

  @Test
  public void groupByTime() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_TIME from ALL_FIELDS group by F_TIME");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(5));
      results.sort(Comparator.naturalOrder());
      assertThat(results.get(0), is("01:20:23"));
      assertThat(LastPlan.INSTANCE.includes(ArrowAggregate.class), is(true));
    }
  }

  @Test
  public void groupByDate() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_DATE from ALL_FIELDS group by F_DATE");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(5));
      results.sort(Comparator.naturalOrder());
      assertThat(results.get(0), is("2020-05-03"));
      assertThat(LastPlan.INSTANCE.includes(ArrowAggregate.class), is(true));
    }
  }

  @Test
  public void groupByDouble() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_DOUBLE from ALL_FIELDS group by F_DOUBLE");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(5));
      results.sort(Comparator.naturalOrder());
      assertThat(results.get(0), is("123.456"));
      assertThat(LastPlan.INSTANCE.includes(ArrowAggregate.class), is(true));
    }
  }

  @Test
  public void groupByIntAndLong() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_INT, F_BIGINT from ALL_FIELDS group by F_INT, F_BIGINT");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(10));
      results.sort(Comparator.naturalOrder());
      assertThat(results.get(9), is("9\t4"));
      assertThat(LastPlan.INSTANCE.includes(ArrowAggregate.class), is(true));
    }
  }

  @Test
  public void groupByLongAndVarchar() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_BIGINT, F_VARCHAR from ALL_FIELDS group by F_BIGINT, F_VARCHAR");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(5));
      results.sort(Comparator.naturalOrder());
      assertThat(results.get(0), is("0\ttest0"));
      assertThat(results.get(4), is("4\ttest4"));
      assertThat(LastPlan.INSTANCE.includes(ArrowAggregate.class), is(true));
    }
  }

  @Test
  public void groupByVarcharAndTimestamp() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_VARCHAR, F_TIMESTAMP from ALL_FIELDS group by F_VARCHAR, F_TIMESTAMP");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(5));
      results.sort(Comparator.naturalOrder());
      assertThat(results.get(0), is("test0\t2020-05-04 13:48:11.0"));
      assertThat(results.get(4), is("test4\t2020-05-08 13:48:11.0"));
      assertThat(LastPlan.INSTANCE.includes(ArrowAggregate.class), is(true));
    }
  }

  @Test
  public void groupByTimestampAndTime() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_TIMESTAMP, F_TIME from ALL_FIELDS group by F_TIMESTAMP, F_TIME");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(5));
      results.sort(Comparator.naturalOrder());
      assertThat(results.get(0), is("2020-05-04 13:48:11.0\t01:20:23"));
      assertThat(results.get(4), is("2020-05-08 13:48:11.0\t05:20:23"));
      assertThat(LastPlan.INSTANCE.includes(ArrowAggregate.class), is(true));
    }
  }

  @Test
  public void groupByTimeAndDate() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_TIME, F_DATE from ALL_FIELDS group by F_TIME, F_DATE");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(5));
      results.sort(Comparator.naturalOrder());
      assertThat(results.get(0), is("01:20:23\t2020-05-03"));
      assertThat(results.get(4), is("05:20:23\t2020-05-07"));
      assertThat(LastPlan.INSTANCE.includes(ArrowAggregate.class), is(true));
    }
  }

  @Test
  public void groupByDateAndDouble() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_DATE, F_DOUBLE from ALL_FIELDS group by F_DATE, F_DOUBLE");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(5));
      results.sort(Comparator.naturalOrder());
      assertThat(results.get(0), is("2020-05-03\t123.456"));
      assertThat(results.get(4), is("2020-05-07\t127.456"));
      assertThat(LastPlan.INSTANCE.includes(ArrowAggregate.class), is(true));
    }
  }

  @Test
  public void groupByDoubleAndInt() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_DOUBLE, F_INT from ALL_FIELDS group by F_DOUBLE, F_INT");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(10));
      results.sort(Comparator.naturalOrder());
      assertThat(results.get(0), is("123.456\t0"));
      assertThat(results.get(9), is("127.456\t9"));
      assertThat(LastPlan.INSTANCE.includes(ArrowAggregate.class), is(true));
    }
  }
}
