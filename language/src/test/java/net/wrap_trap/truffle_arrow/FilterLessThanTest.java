package net.wrap_trap.truffle_arrow;

import java.io.File;
import java.io.IOException;
import java.sql.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;


public class FilterLessThanTest {

  private static final int TEST1_SIZE = 2;
  private static final String TEST1_RESULT = "0\t0\ttest0\t2020-05-04 13:48:11.0\t01:20:23\t2020-05-03\t123.456";

  private static final int TEST2_SIZE = 7;
  private static final String TEST2_RESULT = "3\t3\ttest3\t2020-05-04 16:48:11.0\t04:20:23\t2020-05-06\t126.456";

  @BeforeAll
  public static void setupOnce() throws ClassNotFoundException, IOException {
    Class.forName("net.wrap_trap.truffle_arrow.TruffleDriver");
    TestUtils.generateTestFiles("target/classes/samples/files/all_fields", TestDataType.CASE1);
    TruffleArrowConfig.INSTANCE.reload();
  }

  @AfterAll
  public static void teardownOnce() throws IOException {
    TestUtils.deleteDirectory("target/classes/samples/files/all_fields");
  }

  @Test
  public void byInt() throws SQLException {
    test1("select * from ALL_FIELDS where F_INT < 2");
  }

  @Test
  public void byInt2() throws SQLException {
    test2("select * from ALL_FIELDS where 2 < F_INT");
  }

  @Test
  public void byLong() throws SQLException {
    test1("select * from ALL_FIELDS where F_BIGINT < 2");
  }

  @Test
  public void byLong2() throws SQLException {
    test2("select * from ALL_FIELDS where 2 < F_BIGINT");
  }

  @Test
  public void byString() throws SQLException {
    test1("select * from ALL_FIELDS where F_VARCHAR < 'test2'");
  }

  @Test
  public void byString2() throws SQLException {
    test2("select * from ALL_FIELDS where 'test2' < F_VARCHAR");
  }

  @Test
  public void byTimestamp() throws SQLException {
    test1("select * from ALL_FIELDS where F_TIMESTAMP < timestamp'2020-05-04 15:48:11'");
  }

  @Test
  public void byTimestamp2() throws SQLException {
    test2("select * from ALL_FIELDS where timestamp'2020-05-04 15:48:11' < F_TIMESTAMP");
  }

  @Test
  public void byTime() throws SQLException {
    test1("select * from ALL_FIELDS where F_TIME < time'03:20:23'");
  }

  @Test
  public void byTime2() throws SQLException {
    test2("select * from ALL_FIELDS where time'03:20:23' < F_TIME");
  }

  @Test
  public void byDate() throws SQLException {
    test1("select * from ALL_FIELDS where F_DATE < date'2020-05-05'");
  }

  @Test
  public void byDate2() throws SQLException {
    test2("select * from ALL_FIELDS where date'2020-05-05' < F_DATE");
  }

  @Test
  public void byDouble() throws SQLException {
    test1("select * from ALL_FIELDS where F_DOUBLE < 125.456");
  }

  @Test
  public void byDouble2() throws SQLException {
    test2("select * from ALL_FIELDS where 125.456 < F_DOUBLE");
  }

  private void test1(String sql) throws SQLException {
    TestUtils.filterTest(sql, TEST1_SIZE, TEST1_RESULT);
  }

  private void test2(String sql) throws SQLException {
    TestUtils.filterTest(sql, TEST2_SIZE, TEST2_RESULT);
  }
}