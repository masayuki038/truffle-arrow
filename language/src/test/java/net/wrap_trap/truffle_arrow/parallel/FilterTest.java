package net.wrap_trap.truffle_arrow.parallel;

import java.io.IOException;
import java.sql.*;
import java.util.List;

import net.wrap_trap.truffle_arrow.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


public class FilterTest {
  @BeforeAll
  public static void setupOnce() throws ClassNotFoundException, IOException {
    Class.forName("net.wrap_trap.truffle_arrow.TruffleDriver");
    TestUtils.generateTestFiles("target/classes/samples/files/all_fields", "202009", TestDataType.CASE1);
    TestUtils.generateTestFiles("target/classes/samples/files/all_fields", "202010", TestDataType.CASE2);
    TestUtils.generateTestFiles("target/classes/samples/files/all_fields", "202011", TestDataType.CASE3);
    TestUtils.generateTestFiles("target/classes/samples/files/all_fields", "202012", TestDataType.CASE4);
    TestUtils.generateTestFiles("target/classes/samples/files/all_fields", "202101", TestDataType.CASE5);
    TestUtils.reloadTruffleArrowConfig();
  }

  @AfterAll
  public static void teardownOnce() throws IOException {
    TestUtils.deleteDirectory("target/classes/samples/files/all_fields");
  }

  @Test
  public void equal() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select * from ALL_FIELDS where F_INT=2");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(5));
      assertThat(results, hasItem("2\t2\ttest2\t2020-05-04 15:48:11.0\t03:20:23\t2020-05-05\t125.456"));
      TestUtils.assertRelInclude(ArrowFilter.class);
    }
  }

  @Test
  public void greaterThan() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select * from ALL_FIELDS where F_BIGINT > 2");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(33));
      assertThat(results, hasItem("0\t10\t0\t2020-05-03 00:00:00.0\t01:20:23\t2020-05-03\t10.0"));
      TestUtils.assertRelInclude(ArrowFilter.class);
    }
  }
}
