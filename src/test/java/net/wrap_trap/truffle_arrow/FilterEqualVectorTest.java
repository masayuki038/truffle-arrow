package net.wrap_trap.truffle_arrow;

import java.io.File;
import java.io.IOException;
import java.sql.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class FilterEqualVectorTest {
  private static final String EXPECTED = "0\t0\t0\t2020-05-03 00:00:00.0\t01:20:23\t2020-05-03\t0.0";

  @BeforeAll
  public static void setupOnce() throws ClassNotFoundException, IOException {
    Class.forName("net.wrap_trap.truffle_arrow.TruffleDriver");
    TestUtils.generateTestFiles(
      "target/classes/samples/files/all_fields", TestDataType.CASE3);
    TruffleArrowConfig.INSTANCE.reload();
  }

  @AfterAll
  public static void teardownOnce() throws IOException {
    TestUtils.deleteDirectory("target/classes/samples/files/all_fields");
  }

  @Test
  public void intEqInt() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_INT=F_INT",
      10,
      EXPECTED
    );
  }

  @Test
  public void intEqLong() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_INT=F_BIGINT",
      10,
      EXPECTED
    );
  }

  @Test
  public void intEqString() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_INT=F_VARCHAR",
      10,
      EXPECTED
    );
  }

  @Test
  public void intEqTimestamp() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_INT=F_TIMESTAMP",
       10,
       EXPECTED
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '=' to arguments of type '<INTEGER> = <TIMESTAMP(0)>'"));
  }

  @Test
  public void intEqTime() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_INT=F_TIME",
       10,
       EXPECTED
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '=' to arguments of type '<INTEGER> = <TIME(0)>'"));
  }

  @Test
  public void intEqDate() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_INT=F_DATE",
       10,
       EXPECTED
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '=' to arguments of type '<INTEGER> = <DATE>'"));
  }

  @Test
  public void intEqDouble() throws SQLException {
    TestUtils.filterTest(
     "select * from ALL_FIELDS where F_INT=F_DOUBLE",
     10,
     EXPECTED
    );
  }

  @Test
  public void longEqInt() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_BIGINT=F_INT",
      10,
      EXPECTED
    );
  }

  @Test
  public void longEqLong() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_BIGINT=F_BIGINT",
      10,
      EXPECTED
    );
  }

  @Test
  public void longEqString() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_BIGINT=F_VARCHAR",
      10,
      EXPECTED
    );
  }

  @Test
  public void longEqTimestamp() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
        "select * from ALL_FIELDS where F_BIGINT=F_TIMESTAMP",
        10,
        EXPECTED
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '=' to arguments of type '<BIGINT> = <TIMESTAMP(0)>'"));
  }

  @Test
  public void longEqTime() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_BIGINT=F_TIME",
       10,
       EXPECTED
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '=' to arguments of type '<BIGINT> = <TIME(0)>'"));
  }

  @Test
  public void longEqDate() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_BIGINT=F_DATE",
       10,
       EXPECTED
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '=' to arguments of type '<BIGINT> = <DATE>'"));
  }

  @Test
  public void longEqDouble() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_BIGINT=F_DOUBLE",
      10,
      EXPECTED
    );
  }

  @Test
  public void stringEqInt() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_VARCHAR=F_INT",
      10,
      EXPECTED
    );
  }

  @Test
  public void stringEqLong() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_VARCHAR=F_BIGINT",
      10,
      EXPECTED
    );
  }

  @Test
  public void stringEqString() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_VARCHAR=F_VARCHAR",
      10,
      EXPECTED
    );
  }

  @Test
  public void stringEqTimestamp() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_VARCHAR=F_TIMESTAMP",
       10,
       EXPECTED
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("java.lang.ClassCastException: java.lang.Long cannot be cast to java.lang.String"));
  }

  @Test
  public void stringEqTime() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_VARCHAR=F_TIME",
       10,
       EXPECTED
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("java.lang.ClassCastException: net.wrap_trap.truffle_arrow.type.ArrowTimeSec cannot be cast to java.lang.String"));
  }

  @Test
  public void stringEqDate() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_VARCHAR=F_DATE",
       10,
       EXPECTED
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("java.lang.ClassCastException: java.lang.Integer cannot be cast to java.lang.String"));
  }

  @Test
  public void stringEqDouble() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_VARCHAR=F_DOUBLE",
      10,
      EXPECTED
    );
  }

  @Test
  public void timestampEqInt() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
        "select * from ALL_FIELDS where F_TIMESTAMP=F_INT",
        10,
        EXPECTED
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '=' to arguments of type '<TIMESTAMP(0)> = <INTEGER>'"));
  }

  @Test
  public void timestampEqLong() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
        "select * from ALL_FIELDS where F_TIMESTAMP=F_BIGINT",
        10,
        EXPECTED
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '=' to arguments of type '<TIMESTAMP(0)> = <BIGINT>'"));
  }

  @Test
  public void timestampEqString() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
        "select * from ALL_FIELDS where F_TIMESTAMP=F_VARCHAR",
        10,
        EXPECTED
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("java.lang.ClassCastException: java.lang.String cannot be cast to java.lang.Long"));
  }

  @Test
  public void timestampEqTimestamp() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_TIMESTAMP=F_TIMESTAMP",
      10,
      EXPECTED
    );
  }

  @Test
  public void timestampEqTime() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIMESTAMP=F_TIME",
       10,
       EXPECTED
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '=' to arguments of type '<TIMESTAMP(0)> = <TIME(0)>'"));
  }

  @Test
  public void timestampEqDate() throws SQLException {
    TestUtils.filterTest(
     "select * from ALL_FIELDS where F_TIMESTAMP=F_DATE",
     10,
     EXPECTED
    );
  }

  @Test
  public void timestampEqDouble() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIMESTAMP=F_DOUBLE",
       10,
       EXPECTED
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '=' to arguments of type '<TIMESTAMP(0)> = <DOUBLE>'"));
  }

  @Test
  public void timeEqInt() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIME=F_INT",
       10,
       EXPECTED
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '=' to arguments of type '<TIME(0)> = <INTEGER>'"));
  }

  @Test
  public void timeEqLong() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIME=F_BIGINT",
       10,
       EXPECTED
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '=' to arguments of type '<TIME(0)> = <BIGINT>'"));
  }

  @Test
  public void timeEqString() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIME=F_VARCHAR",
       10,
       EXPECTED
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("java.lang.ClassCastException: java.lang.String cannot be cast to net.wrap_trap.truffle_arrow.type.ArrowTimeSec"));
  }

  @Test
  public void timeEqTimestamp() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIME=F_TIMESTAMP",
       10,
       EXPECTED
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '=' to arguments of type '<TIME(0)> = <TIMESTAMP(0)>'"));
  }

  @Test
  public void timeEqTime() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_TIME=F_TIME",
      10,
      EXPECTED
    );
  }

  @Test
  public void timeEqDate() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIME=F_DATE",
       10,
       EXPECTED
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '=' to arguments of type '<TIME(0)> = <DATE>'"));
  }

  @Test
  public void timeEqDouble() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIME=F_DOUBLE",
       10,
       EXPECTED
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '=' to arguments of type '<TIME(0)> = <DOUBLE>'"));
  }

  @Test
  public void dateEqInt() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_DATE=F_INT",
       10,
       EXPECTED
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '=' to arguments of type '<DATE> = <INTEGER>'"));
  }

  @Test
  public void dateEqLong() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_DATE=F_BIGINT",
       10,
       EXPECTED
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '=' to arguments of type '<DATE> = <BIGINT>'"));
  }

  @Test
  public void dateEqString() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
        "select * from ALL_FIELDS where F_DATE=F_VARCHAR",
        10,
      EXPECTED
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("java.lang.ClassCastException: java.lang.String cannot be cast to java.lang.Integer"));
  }

  @Test
  public void dateEqTimestamp() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_DATE=F_TIMESTAMP",
      10,
      EXPECTED
    );
  }

  @Test
  public void dateEqTime() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_DATE=F_TIME",
       10,
       EXPECTED
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '=' to arguments of type '<DATE> = <TIME(0)>'"));
  }

  @Test
  public void dateEqDate() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_DATE=F_DATE",
      10,
      EXPECTED
    );
  }

  @Test
  public void dateEqDouble() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_DATE=F_DOUBLE",
       10,
       EXPECTED
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '=' to arguments of type '<DATE> = <DOUBLE>'"));
  }

  @Test
  public void doubleEqInt() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_DOUBLE=F_INT",
      10,
      EXPECTED
    );
  }

  @Test
  public void doubleEqLong() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_DOUBLE=F_BIGINT",
      10,
      EXPECTED
    );
  }

  @Test
  public void doubleEqString() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_DOUBLE=F_VARCHAR",
      10,
      EXPECTED
    );
  }

  @Test
  public void doubleEqTimestamp() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_DOUBLE=F_TIMESTAMP",
       10,
       EXPECTED
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '=' to arguments of type '<DOUBLE> = <TIMESTAMP(0)>'"));
  }

  @Test
  public void doubleEqTime() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_DOUBLE=F_TIME",
       10,
       EXPECTED
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '=' to arguments of type '<DOUBLE> = <TIME(0)>'"));
  }

  @Test
  public void doubleEqDate() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
        "select * from ALL_FIELDS where F_DOUBLE=F_DATE",
        10,
      EXPECTED
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '=' to arguments of type '<DOUBLE> = <DATE>'"));
  }

  @Test
  public void doubleEqDouble() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_DOUBLE=F_DOUBLE",
      10,
      EXPECTED
    );
  }
}
