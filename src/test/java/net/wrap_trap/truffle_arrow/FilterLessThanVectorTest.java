package net.wrap_trap.truffle_arrow;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class FilterLessThanVectorTest {
  private static final String EXPECTED0 = "0\t10\t0\t2020-05-03 00:00:00.0\t01:20:23\t2020-05-03\t10.0";
  private static final String EXPECTED6 = "6\t4\t6\t2020-05-09 00:00:00.0\t07:20:23\t2020-05-09\t4.0";

  @BeforeAll
  public static void setupOnce() throws ClassNotFoundException, IOException {
    Class.forName("net.wrap_trap.truffle_arrow.TruffleDriver");
    TestUtils.generateTestFile(
      "target/classes/samples/files/all_fields.arrow", TestDataType.CASE4);
    TruffleArrowConfig.INSTANCE.reload();
  }

  @AfterAll
  public static void teardownOnce() {
    new File("target/classes/samples/files/all_fields.arrow").delete();
  }

  @Test
  public void intLtInt() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_INT < F_INT",
      0
    );
  }

  @Test
  public void intLtLong() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where CAST(F_INT AS BIGINT) < F_BIGINT",
      5,
      EXPECTED0
    );
  }

  @Test
  public void intLtLong2() throws SQLException {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_INT < CAST(F_BIGINT AS INT)",
       5,
       EXPECTED0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Unsupported operation: CAST(class java.lang.Long As Int)"));
  }

  @Test
  public void intLtString() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_INT < F_VARCHAR",
      0
    );
  }

  @Test
  public void intLtTimestamp() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
        "select * from ALL_FIELDS where F_INT < F_TIMESTAMP",
        10
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<' to arguments of type '<INTEGER> < <TIMESTAMP(0)>'"));
  }

  @Test
  public void intLtTime() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_INT < F_TIME",
       10
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<' to arguments of type '<INTEGER> < <TIME(0)>'"));
  }

  @Test
  public void intLtDate() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_INT < F_DATE",
       10
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<' to arguments of type '<INTEGER> < <DATE>'"));
  }

  @Test
  public void intLtDouble() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where CAST(F_INT AS DOUBLE) < F_DOUBLE",
      5,
      EXPECTED0
    );
  }

  @Test
  public void intLtDouble2() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
        "select * from ALL_FIELDS where F_INT < CAST(F_DOUBLE AS INT)",
        5,
        EXPECTED0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Unsupported operation: CAST(class java.lang.Double As Int)"));
  }

  @Test
  public void longLtInt() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_BIGINT < CAST(F_INT AS BigInt)",
      4,
      EXPECTED6
    );
  }

  @Test
  public void longLtLong() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_BIGINT < F_BIGINT",
      0
    );
  }

  @Test
  public void longLtString() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_BIGINT < F_VARCHAR",
      4,
      EXPECTED6
    );
  }

  @Test
  public void longLtTimestamp() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
      "select * from ALL_FIELDS where F_BIGINT < F_TIMESTAMP",
      10
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<' to arguments of type '<BIGINT> < <TIMESTAMP(0)>'"));
  }

  @Test
  public void longLtTime() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_BIGINT < F_TIME",
       10
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<' to arguments of type '<BIGINT> < <TIME(0)>'"));
  }

  @Test
  public void longLtDate() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_BIGINT < F_DATE",
       10
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<' to arguments of type '<BIGINT> < <DATE>'"));
  }

  @Test
  public void longLtDouble() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where CAST(F_BIGINT AS DOUBLE) < F_DOUBLE",
      0
    );
  }

  @Test
  public void longLtDouble2() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_BIGINT < CAST(F_DOUBLE AS BIGINT)",
       5,
       EXPECTED0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Unsupported operation: CAST(class java.lang.Double As Long)"));
  }

  @Test
  public void stringLtInt() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_VARCHAR < F_INT",
      0
    );
  }

  @Test
  public void stringLtLong() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_VARCHAR < F_BIGINT",
      5,
      EXPECTED0
    );
  }

  @Test
  public void stringLtString() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_VARCHAR < F_VARCHAR",
      0
    );
  }

  @Test
  public void stringLtTimestamp() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_VARCHAR < F_TIMESTAMP",
       10
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Unsupported operation: CAST(class org.apache.arrow.vector.util.Text As Timestamp)"));
  }

  @Test
  public void stringLtTime() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_VARCHAR < F_TIME",
       10
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Unsupported operation: CAST(class org.apache.arrow.vector.util.Text As TIME)"));
  }

  @Test
  public void stringLtDate() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
      "select * from ALL_FIELDS where F_VARCHAR < F_DATE",
      10
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Unsupported operation: CAST(class org.apache.arrow.vector.util.Text As DATE)"));
  }

  @Test
  public void stringLtDouble() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_VARCHAR < F_DOUBLE",
      5,
      EXPECTED0
    );
  }

  @Test
  public void timestampLtInt() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIMESTAMP < F_INT",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<' to arguments of type '<TIMESTAMP(0)> < <INTEGER>'"));
  }

  @Test
  public void timestampLtLong() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIMESTAMP < F_BIGINT",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<' to arguments of type '<TIMESTAMP(0)> < <BIGINT>'"));
  }

  @Test
  public void timestampLtString() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIMESTAMP < F_VARCHAR",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Unsupported operation: CAST(class org.apache.arrow.vector.util.Text As Timestamp)"));
  }

  @Test
  public void timestampLtTimestamp() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_TIMESTAMP < F_TIMESTAMP",
      0
    );
  }

  @Test
  public void timestampLtTime() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIMESTAMP < F_TIME",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<' to arguments of type '<TIMESTAMP(0)> < <TIME(0)>'"));
  }

  @Test
  public void timestampLtDate() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_TIMESTAMP < F_DATE",
      0
    );
  }

  @Test
  public void timestampLtDouble() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIMESTAMP < F_DOUBLE",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<' to arguments of type '<TIMESTAMP(0)> < <DOUBLE>'"));
  }

  @Test
  public void timeLtInt() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIME < F_INT",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<' to arguments of type '<TIME(0)> < <INTEGER>'"));
  }

  @Test
  public void timeLtLong() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIME < F_BIGINT",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<' to arguments of type '<TIME(0)> < <BIGINT>'"));
  }

  @Test
  public void timeLtString() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIME < F_VARCHAR",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Unsupported operation: CAST(class org.apache.arrow.vector.util.Text As TIME)"));
  }

  @Test
  public void timeLtTimestamp() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIME < F_TIMESTAMP",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<' to arguments of type '<TIME(0)> < <TIMESTAMP(0)>'"));
  }

  @Test
  public void timeLtTime() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_TIME < F_TIME",
      0
    );
  }

  @Test
  public void timeLtDate() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIME < F_DATE",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<' to arguments of type '<TIME(0)> < <DATE>'"));
  }

  @Test
  public void timeLtDouble() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
        "select * from ALL_FIELDS where F_TIME < F_DOUBLE",
        0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<' to arguments of type '<TIME(0)> < <DOUBLE>'"));
  }

  @Test
  public void dateLtInt() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_DATE < F_INT",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<' to arguments of type '<DATE> < <INTEGER>'"));
  }

  @Test
  public void dateLtLong() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_DATE < F_BIGINT",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<' to arguments of type '<DATE> < <BIGINT>'"));
  }

  @Test
  public void dateLtString() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_DATE < F_VARCHAR",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Unsupported operation: CAST(class org.apache.arrow.vector.util.Text As DATE)"));
  }

  @Test
  public void dateLtTimestamp() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_DATE < F_TIMESTAMP",
      0
    );
  }

  @Test
  public void dateLtTime() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
        "select * from ALL_FIELDS where F_DATE < F_TIME",
        0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<' to arguments of type '<DATE> < <TIME(0)>'"));
  }

  @Test
  public void dateLtDate() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_DATE < F_DATE",
      0
    );
  }

  @Test
  public void dateLtDouble() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_DATE < F_DOUBLE",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<' to arguments of type '<DATE> < <DOUBLE>'"));
  }

  @Test
  public void doubleLtInt() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_DOUBLE < CAST(F_INT AS Double)",
      4,
      EXPECTED6
    );
  }

  @Test
  public void doubleLtLong() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_DOUBLE < CAST(F_BIGINT AS Double)",
      0
    );
  }

  @Test
  public void doubleLtString() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_DOUBLE < F_VARCHAR",
      4,
      EXPECTED6
    );
  }

  @Test
  public void doubleLtTimestamp() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_DOUBLE < F_TIMESTAMP",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<' to arguments of type '<DOUBLE> < <TIMESTAMP(0)>'"));
  }

  @Test
  public void doubleLtTime() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_DOUBLE < F_TIME",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<' to arguments of type '<DOUBLE> < <TIME(0)>'"));
  }

  @Test
  public void doubleLtDate() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_DOUBLE < F_DATE",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<' to arguments of type '<DOUBLE> < <DATE>'"));
  }

  @Test
  public void doubleLtDouble() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_DOUBLE < F_DOUBLE",
      0
    );
  }
}
