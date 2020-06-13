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

public class FilterGreaterEqualVectorTest {
  private static final String EXPECTED0 = "0\t10\t0\t2020-05-03 00:00:00.0\t01:20:23\t2020-05-03\t10.0";
  private static final String EXPECTED5 = "5\t5\t5\t2020-05-08 00:00:00.0\t06:20:23\t2020-05-08\t5.0";

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
  public void intGeInt() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_INT >= F_INT",
      10
    );
  }

  @Test
  public void intGeLong() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where CAST(F_INT AS BIGINT) >= F_BIGINT",
      5,
      EXPECTED5
    );
  }

  @Test
  public void intGeLong2() throws SQLException {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_INT >= CAST(F_BIGINT AS INT)",
       5,
       EXPECTED5
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Unsupported operation: CAST(class java.lang.Long As Int)"));
  }

  @Test
  public void intGeString() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_INT >= F_VARCHAR",
      10
    );
  }

  @Test
  public void intGeTimestamp() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
        "select * from ALL_FIELDS where F_INT >= F_TIMESTAMP",
        10
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>=' to arguments of type '<INTEGER> >= <TIMESTAMP(0)>'"));
  }

  @Test
  public void intGeTime() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_INT >= F_TIME",
       10
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>=' to arguments of type '<INTEGER> >= <TIME(0)>'"));
  }

  @Test
  public void intGeDate() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_INT >= F_DATE",
       10
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>=' to arguments of type '<INTEGER> >= <DATE>'"));
  }

  @Test
  public void intGeDouble() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where CAST(F_INT AS DOUBLE) >= F_DOUBLE",
      5,
      EXPECTED5
    );
  }

  @Test
  public void intGeDouble2() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
        "select * from ALL_FIELDS where F_INT >= CAST(F_DOUBLE AS INT)",
        5,
        EXPECTED5
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Unsupported operation: CAST(class java.lang.Double As Int)"));
  }

  @Test
  public void longGeInt() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_BIGINT >= CAST(F_INT AS BigInt)",
      6,
      EXPECTED0
    );
  }

  @Test
  public void longGeLong() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_BIGINT >= F_BIGINT",
      10
    );
  }

  @Test
  public void longGeString() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_BIGINT >= F_VARCHAR",
      6,
      EXPECTED0
    );
  }

  @Test
  public void longGeTimestamp() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
      "select * from ALL_FIELDS where F_BIGINT >= F_TIMESTAMP",
      10
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>=' to arguments of type '<BIGINT> >= <TIMESTAMP(0)>'"));
  }

  @Test
  public void longGeTime() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_BIGINT >= F_TIME",
       10
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>=' to arguments of type '<BIGINT> >= <TIME(0)>'"));
  }

  @Test
  public void longGeDate() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_BIGINT >= F_DATE",
       10
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>=' to arguments of type '<BIGINT> >= <DATE>'"));
  }

  @Test
  public void longGeDouble() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where CAST(F_BIGINT AS DOUBLE) >= F_DOUBLE",
      10
    );
  }

  @Test
  public void longGeDouble2() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_BIGINT >= CAST(F_DOUBLE AS BIGINT)",
       5,
       EXPECTED5
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Unsupported operation: CAST(class java.lang.Double As Long)"));
  }

  @Test
  public void stringGeInt() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_VARCHAR >= F_INT",
      10
    );
  }

  @Test
  public void stringGeLong() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_VARCHAR >= F_BIGINT",
      5,
      EXPECTED5
    );
  }

  @Test
  public void stringGeString() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_VARCHAR >= F_VARCHAR",
      10
    );
  }

  @Test
  public void stringGeTimestamp() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_VARCHAR >= F_TIMESTAMP",
       10
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Unsupported operation: CAST(class org.apache.arrow.vector.util.Text As Timestamp)"));
  }

  @Test
  public void stringGeTime() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_VARCHAR >= F_TIME",
       10
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Unsupported operation: CAST(class org.apache.arrow.vector.util.Text As TIME)"));
  }

  @Test
  public void stringGeDate() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
      "select * from ALL_FIELDS where F_VARCHAR >= F_DATE",
      10
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Unsupported operation: CAST(class org.apache.arrow.vector.util.Text As DATE)"));
  }

  @Test
  public void stringGeDouble() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_VARCHAR >= F_DOUBLE",
      5,
      EXPECTED5
    );
  }

  @Test
  public void timestampGeInt() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIMESTAMP >= F_INT",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>=' to arguments of type '<TIMESTAMP(0)> >= <INTEGER>'"));
  }

  @Test
  public void timestampGeLong() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIMESTAMP >= F_BIGINT",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>=' to arguments of type '<TIMESTAMP(0)> >= <BIGINT>'"));
  }

  @Test
  public void timestampGeString() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIMESTAMP >= F_VARCHAR",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("CAST(class org.apache.arrow.vector.util.Text As Timestamp)"));
  }

  @Test
  public void timestampGeTimestamp() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_TIMESTAMP >= F_TIMESTAMP",
      10
    );
  }

  @Test
  public void timestampGeTime() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIMESTAMP >= F_TIME",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>=' to arguments of type '<TIMESTAMP(0)> >= <TIME(0)>'"));
  }

  @Test
  public void timestampGeDate() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_TIMESTAMP >= F_DATE",
      10
    );
  }

  @Test
  public void timestampGeDouble() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIMESTAMP >= F_DOUBLE",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>=' to arguments of type '<TIMESTAMP(0)> >= <DOUBLE>'"));
  }

  @Test
  public void timeGeInt() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIME >= F_INT",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>=' to arguments of type '<TIME(0)> >= <INTEGER>'"));
  }

  @Test
  public void timeGeLong() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIME >= F_BIGINT",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>=' to arguments of type '<TIME(0)> >= <BIGINT>'"));
  }

  @Test
  public void timeGeString() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIME >= F_VARCHAR",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Unsupported operation: CAST(class org.apache.arrow.vector.util.Text As TIME)"));
  }

  @Test
  public void timeGeTimestamp() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIME >= F_TIMESTAMP",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>=' to arguments of type '<TIME(0)> >= <TIMESTAMP(0)>'"));
  }

  @Test
  public void timeGeTime() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_TIME >= F_TIME",
      10
    );
  }

  @Test
  public void timeGeDate() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIME >= F_DATE",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>=' to arguments of type '<TIME(0)> >= <DATE>'"));
  }

  @Test
  public void timeGeDouble() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
        "select * from ALL_FIELDS where F_TIME >= F_DOUBLE",
        0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>=' to arguments of type '<TIME(0)> >= <DOUBLE>'"));
  }

  @Test
  public void dateGeInt() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_DATE >= F_INT",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>=' to arguments of type '<DATE> >= <INTEGER>'"));
  }

  @Test
  public void dateGeLong() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_DATE >= F_BIGINT",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>=' to arguments of type '<DATE> >= <BIGINT>'"));
  }

  @Test
  public void dateGeString() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_DATE >= F_VARCHAR",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Unsupported operation: CAST(class org.apache.arrow.vector.util.Text As DATE)"));
  }

  @Test
  public void dateGeTimestamp() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_DATE >= F_TIMESTAMP",
      10
    );
  }

  @Test
  public void dateGeTime() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
        "select * from ALL_FIELDS where F_DATE >= F_TIME",
        0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>=' to arguments of type '<DATE> >= <TIME(0)>'"));
  }

  @Test
  public void dateGeDate() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_DATE >= F_DATE",
      10
    );
  }

  @Test
  public void dateGeDouble() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_DATE >= F_DOUBLE",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>=' to arguments of type '<DATE> >= <DOUBLE>'"));
  }

  @Test
  public void doubleGeInt() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_DOUBLE >= CAST(F_INT AS Double)",
      6,
      EXPECTED0
    );
  }

  @Test
  public void doubleGeLong() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_DOUBLE >= CAST(F_BIGINT AS Double)",
      10
    );
  }

  @Test
  public void doubleGeString() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_DOUBLE >= F_VARCHAR",
      6,
      EXPECTED0
    );
  }

  @Test
  public void doubleGeTimestamp() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_DOUBLE >= F_TIMESTAMP",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>=' to arguments of type '<DOUBLE> >= <TIMESTAMP(0)>'"));
  }

  @Test
  public void doubleGeTime() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_DOUBLE >= F_TIME",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>=' to arguments of type '<DOUBLE> >= <TIME(0)>'"));
  }

  @Test
  public void doubleGeDate() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_DOUBLE >= F_DATE",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>=' to arguments of type '<DOUBLE> >= <DATE>'"));
  }

  @Test
  public void doubleGeDouble() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_DOUBLE >= F_DOUBLE",
      10
    );
  }
}
