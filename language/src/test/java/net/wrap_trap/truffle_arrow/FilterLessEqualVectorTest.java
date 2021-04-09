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

public class FilterLessEqualVectorTest {
  private static final String EXPECTED0 = "0\t10\t0\t2020-05-03 00:00:00.0\t01:20:23\t2020-05-03\t10.0";
  private static final String EXPECTED5 = "5\t5\t5\t2020-05-08 00:00:00.0\t06:20:23\t2020-05-08\t5.0";

  @BeforeAll
  public static void setupOnce() throws ClassNotFoundException, IOException {
    Class.forName("net.wrap_trap.truffle_arrow.TruffleDriver");
    TestUtils.generateTestFiles(
      "target/classes/samples/files/all_fields", TestDataType.CASE4);
    TruffleArrowConfig.INSTANCE.reload();
  }

  @AfterAll
  public static void teardownOnce() throws IOException {
    TestUtils.deleteDirectory("target/classes/samples/files/all_fields");
  }

  @Test
  public void intLeInt() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_INT <= F_INT",
      10
    );
  }

  @Test
  public void intLeLong() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where CAST(F_INT AS BIGINT) <= F_BIGINT",
      6,
      EXPECTED0
    );
  }

  @Test
  public void intLeLong2() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_INT <= CAST(F_BIGINT AS INT)",
      6,
      EXPECTED0
    );
  }

  @Test
  public void intLeString() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_INT <= F_VARCHAR",
      10
    );
  }

  @Test
  public void intLeTimestamp() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
        "select * from ALL_FIELDS where F_INT <= F_TIMESTAMP",
        10
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<=' to arguments of type '<INTEGER> <= <TIMESTAMP(0)>'"));
  }

  @Test
  public void intLeTime() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_INT <= F_TIME",
       10
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<=' to arguments of type '<INTEGER> <= <TIME(0)>'"));
  }

  @Test
  public void intLeDate() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_INT <= F_DATE",
       10
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<=' to arguments of type '<INTEGER> <= <DATE>'"));
  }

  @Test
  public void intLeDouble() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where CAST(F_INT AS DOUBLE) <= F_DOUBLE",
      6,
      EXPECTED0
    );
  }

  @Test
  public void intLeDouble2() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
        "select * from ALL_FIELDS where F_INT <= CAST(F_DOUBLE AS INT)",
        6,
        EXPECTED0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Unsupported operation: CAST(class java.lang.Double As Int)"));
  }

  @Test
  public void longLeInt() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_BIGINT <= CAST(F_INT AS BigInt)",
      5,
      EXPECTED5
    );
  }

  @Test
  public void longLeLong() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_BIGINT <= F_BIGINT",
      10
    );
  }

  @Test
  public void longLeString() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_BIGINT <= F_VARCHAR",
      5,
      EXPECTED5
    );
  }

  @Test
  public void longLeTimestamp() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
      "select * from ALL_FIELDS where F_BIGINT <= F_TIMESTAMP",
      10
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<=' to arguments of type '<BIGINT> <= <TIMESTAMP(0)>'"));
  }

  @Test
  public void longLeTime() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_BIGINT <= F_TIME",
       10
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<=' to arguments of type '<BIGINT> <= <TIME(0)>'"));
  }

  @Test
  public void longLeDate() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_BIGINT <= F_DATE",
       10
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<=' to arguments of type '<BIGINT> <= <DATE>'"));
  }

  @Test
  public void longLeDouble() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where CAST(F_BIGINT AS DOUBLE) <= F_DOUBLE",
      10
    );
  }

  @Test
  public void longLeDouble2() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_BIGINT <= CAST(F_DOUBLE AS BIGINT)",
       6,
       EXPECTED0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Unsupported operation: CAST(class java.lang.Double As Long)"));
  }

  @Test
  public void stringLeInt() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_VARCHAR <= F_INT",
      10
    );
  }

  @Test
  public void stringLeLong() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_VARCHAR <= F_BIGINT",
      6,
      EXPECTED0
    );
  }

  @Test
  public void stringLeString() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_VARCHAR <= F_VARCHAR",
      10
    );
  }

  @Test
  public void stringLeTimestamp() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_VARCHAR <= F_TIMESTAMP",
       10
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Unsupported operation: CAST(class org.apache.arrow.vector.util.Text As Timestamp)"));
  }

  @Test
  public void stringLeTime() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_VARCHAR <= F_TIME",
       10
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Unsupported operation: CAST(class org.apache.arrow.vector.util.Text As TIME)"));
  }

  @Test
  public void stringLeDate() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
      "select * from ALL_FIELDS where F_VARCHAR <= F_DATE",
      10
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Unsupported operation: CAST(class org.apache.arrow.vector.util.Text As DATE)"));
  }

  @Test
  public void stringLeDouble() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_VARCHAR <= F_DOUBLE",
      6,
      EXPECTED0
    );
  }

  @Test
  public void timestampLeInt() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIMESTAMP <= F_INT",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<=' to arguments of type '<TIMESTAMP(0)> <= <INTEGER>'"));
  }

  @Test
  public void timestampLeLong() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIMESTAMP <= F_BIGINT",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<=' to arguments of type '<TIMESTAMP(0)> <= <BIGINT>'"));
  }

  @Test
  public void timestampLeString() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIMESTAMP <= F_VARCHAR",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Unsupported operation: CAST(class org.apache.arrow.vector.util.Text As Timestamp)"));
  }

  @Test
  public void timestampLeTimestamp() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_TIMESTAMP <= F_TIMESTAMP",
      10
    );
  }

  @Test
  public void timestampLeTime() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIMESTAMP <= F_TIME",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<=' to arguments of type '<TIMESTAMP(0)> <= <TIME(0)>'"));
  }

  @Test
  public void timestampLeDate() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_TIMESTAMP <= F_DATE",
      10
    );
  }

  @Test
  public void timestampLeDouble() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIMESTAMP <= F_DOUBLE",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<=' to arguments of type '<TIMESTAMP(0)> <= <DOUBLE>'"));
  }

  @Test
  public void timeLeInt() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIME <= F_INT",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<=' to arguments of type '<TIME(0)> <= <INTEGER>'"));
  }

  @Test
  public void timeLeLong() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIME <= F_BIGINT",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<=' to arguments of type '<TIME(0)> <= <BIGINT>'"));
  }

  @Test
  public void timeLeString() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIME <= F_VARCHAR",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Unsupported operation: CAST(class org.apache.arrow.vector.util.Text As TIME)"));
  }

  @Test
  public void timeLeTimestamp() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIME <= F_TIMESTAMP",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<=' to arguments of type '<TIME(0)> <= <TIMESTAMP(0)>'"));
  }

  @Test
  public void timeLeTime() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_TIME <= F_TIME",
      10
    );
  }

  @Test
  public void timeLeDate() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIME <= F_DATE",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<=' to arguments of type '<TIME(0)> <= <DATE>'"));
  }

  @Test
  public void timeLeDouble() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
        "select * from ALL_FIELDS where F_TIME <= F_DOUBLE",
        0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<=' to arguments of type '<TIME(0)> <= <DOUBLE>'"));
  }

  @Test
  public void dateLeInt() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_DATE <= F_INT",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<=' to arguments of type '<DATE> <= <INTEGER>'"));
  }

  @Test
  public void dateLeLong() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_DATE <= F_BIGINT",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<=' to arguments of type '<DATE> <= <BIGINT>'"));
  }

  @Test
  public void dateLeString() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_DATE <= F_VARCHAR",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Unsupported operation: CAST(class org.apache.arrow.vector.util.Text As DATE)"));
  }

  @Test
  public void dateLeTimestamp() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_DATE <= F_TIMESTAMP",
      10
    );
  }

  @Test
  public void dateLeTime() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
        "select * from ALL_FIELDS where F_DATE <= F_TIME",
        0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<=' to arguments of type '<DATE> <= <TIME(0)>'"));
  }

  @Test
  public void dateLeDate() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_DATE <= F_DATE",
      10
    );
  }

  @Test
  public void dateLeDouble() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_DATE <= F_DOUBLE",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<=' to arguments of type '<DATE> <= <DOUBLE>'"));
  }

  @Test
  public void doubleLeInt() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_DOUBLE <= CAST(F_INT AS Double)",
      5,
      EXPECTED5
    );
  }

  @Test
  public void doubleLeLong() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_DOUBLE <= CAST(F_BIGINT AS Double)",
      10
    );
  }

  @Test
  public void doubleLeString() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_DOUBLE <= F_VARCHAR",
      5,
      EXPECTED5
    );
  }

  @Test
  public void doubleLeTimestamp() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_DOUBLE <= F_TIMESTAMP",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<=' to arguments of type '<DOUBLE> <= <TIMESTAMP(0)>'"));
  }

  @Test
  public void doubleLeTime() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_DOUBLE <= F_TIME",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<=' to arguments of type '<DOUBLE> <= <TIME(0)>'"));
  }

  @Test
  public void doubleLeDate() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_DOUBLE <= F_DATE",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '<=' to arguments of type '<DOUBLE> <= <DATE>'"));
  }

  @Test
  public void doubleLeDouble() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_DOUBLE <= F_DOUBLE",
      10
    );
  }
}
