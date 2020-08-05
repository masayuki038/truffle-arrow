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
    new File("target/classes/samples/files/all_fields.arrow").delete();
  }

  @Test
  public void groupByInt() throws SQLException {
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
}
