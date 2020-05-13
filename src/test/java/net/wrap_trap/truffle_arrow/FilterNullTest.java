package net.wrap_trap.truffle_arrow;

import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.*;


public class FilterNullTest {
  @BeforeClass
  public static void setupOnce() throws ClassNotFoundException, IOException {
    Class.forName("net.wrap_trap.truffle_arrow.TruffleDriver");
    TestUtils.generateTestFile("target/classes/samples/files/all_nullable_fields.arrow", true);
  }

  @AfterClass
  public static void teardownOnce() {
    new File("target/classes/samples/files/all_nullable_fields.arrow").delete();
  }

  @Test
  public void byInt() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_NULLABLE_FIELDS where F_INT IS NULL",
      1,
      "_NULL_\t1\ttest1\t2020-05-04 14:48:11.0\t02:20:23\t2020-05-04\t124.456");
  }

  @Test
  public void byLong() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_NULLABLE_FIELDS where F_BIGINT IS NULL",
      1,
      "2\t_NULL_\ttest2\t2020-05-04 15:48:11.0\t03:20:23\t2020-05-05\t125.456");
  }

  @Test
  public void byString() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_NULLABLE_FIELDS where F_VARCHAR IS NULL",
      1,
      "3\t3\t_NULL_\t2020-05-04 16:48:11.0\t04:20:23\t2020-05-06\t126.456");
  }

  @Test
  public void byTimestamp() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_NULLABLE_FIELDS where F_TIMESTAMP IS NULL",
      1,
      "4\t4\ttest4\t_NULL_\t05:20:23\t2020-05-07\t127.456");
  }

  @Test
  public void byTime() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_NULLABLE_FIELDS where F_TIME IS NULL",
      1,
      "5\t5\ttest5\t2020-05-04 18:48:11.0\t_NULL_\t2020-05-08\t128.45600000000002");
  }

  @Test
  public void byDate() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_NULLABLE_FIELDS where F_DATE IS NULL",
      1,
      "6\t6\ttest6\t2020-05-04 19:48:11.0\t07:20:23\t_NULL_\t129.45600000000002");
  }

  @Test
  public void byDouble() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_NULLABLE_FIELDS where F_DOUBLE IS NULL",
      1,
      "7\t7\ttest7\t2020-05-04 20:48:11.0\t08:20:23\t2020-05-10\t_NULL_");
  }
}
