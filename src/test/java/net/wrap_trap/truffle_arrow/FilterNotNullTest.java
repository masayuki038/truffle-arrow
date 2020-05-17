package net.wrap_trap.truffle_arrow;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

public class FilterNotNullTest {

  private static final String EXPECTED = "0\t0\ttest0\t2020-05-04 13:48:11.0\t01:20:23\t2020-05-03\t123.456";

  @BeforeClass
  public static void setupOnce() throws ClassNotFoundException, IOException {
    Class.forName("net.wrap_trap.truffle_arrow.TruffleDriver");
    TestUtils.generateTestFile("target/classes/samples/files/all_nullable_fields.arrow", true);
    TruffleArrowConfig.INSTANCE.reload();
  }

  @AfterClass
  public static void teardownOnce() {
    new File("target/classes/samples/files/all_nullable_fields.arrow").delete();
  }

  @Test
  public void byInt() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_NULLABLE_FIELDS where F_INT IS NOT NULL",
      9,
      EXPECTED
    );
  }

  @Test
  public void byLong() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_NULLABLE_FIELDS where F_BIGINT IS NOT NULL",
      9,
      EXPECTED);
  }

  @Test
  public void byString() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_NULLABLE_FIELDS where F_VARCHAR IS NOT NULL",
      9,
      EXPECTED);
  }

  @Test
  public void byTimestamp() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_NULLABLE_FIELDS where F_TIMESTAMP IS NOT NULL",
      9,
      EXPECTED);
  }

  @Test
  public void byTime() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_NULLABLE_FIELDS where F_TIME IS NOT NULL",
      9,
      EXPECTED);
  }

  @Test
  public void byDate() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_NULLABLE_FIELDS where F_DATE IS NOT NULL",
      9,
      EXPECTED);
  }

  @Test
  public void byDouble() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_NULLABLE_FIELDS where F_DOUBLE IS NOT NULL",
      9,
      EXPECTED);
  }
}
