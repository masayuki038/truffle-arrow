package net.wrap_trap.truffle_arrow;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.sql.*;


public class FilterAndTest {

  private static final String EXPECTED0 = "0\t10\t0\t2020-05-03 00:00:00.0\t01:20:23\t2020-05-03\t10.0";

  @BeforeAll
  public static void setupOnce() throws ClassNotFoundException, IOException {
    Class.forName("net.wrap_trap.truffle_arrow.TruffleDriver");
    TestUtils.generateTestFile("target/classes/samples/files/all_fields.arrow", TestDataType.CASE4);
    TruffleArrowConfig.INSTANCE.reload();
  }

  @AfterAll
  public static void teardownOnce() {
    new File("target/classes/samples/files/all_fields.arrow").delete();
  }

  @Test
  public void bothAreTrue() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_INT=0 AND F_BIGINT=10",
      1,
      EXPECTED0
    );
  }

  @Test
  public void oneIsTrueButOtherIsFalse() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_INT=0 AND F_BIGINT=9",
      0
    );
  }

  @Test
  public void bothAreFalse() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_INT=-1 AND F_BIGINT=-1",
      0
    );
  }
}
