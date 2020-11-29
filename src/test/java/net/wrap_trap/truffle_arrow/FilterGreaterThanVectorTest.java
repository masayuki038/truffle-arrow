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

public class FilterGreaterThanVectorTest {
  private static final String EXPECTED0 = "0\t10\t0\t2020-05-03 00:00:00.0\t01:20:23\t2020-05-03\t10.0";
  private static final String EXPECTED6 = "6\t4\t6\t2020-05-09 00:00:00.0\t07:20:23\t2020-05-09\t4.0";

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
  public void intGtInt() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_INT > F_INT",
      0
    );
  }

  @Test
  public void intGtLong() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where CAST(F_INT AS BIGINT) > F_BIGINT",
      4,
      EXPECTED6
    );
  }

  @Test
  public void intGtLong2() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_INT > CAST(F_BIGINT AS INT)",
      4,
      EXPECTED6
    );
  }

  @Test
  public void intGtString() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_INT > F_VARCHAR",
      0
    );
  }

  @Test
  public void intGtTimestamp() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
        "select * from ALL_FIELDS where F_INT > F_TIMESTAMP",
        10
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>' to arguments of type '<INTEGER> > <TIMESTAMP(0)>'"));
  }

  @Test
  public void intGtTime() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_INT > F_TIME",
       10
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>' to arguments of type '<INTEGER> > <TIME(0)>'"));
  }

  @Test
  public void intGtDate() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_INT > F_DATE",
       10
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>' to arguments of type '<INTEGER> > <DATE>'"));
  }

  @Test
  public void intGtDouble() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where CAST(F_INT AS DOUBLE) > F_DOUBLE",
      4,
      EXPECTED6
    );
  }

  @Test
  public void intGtDouble2() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
        "select * from ALL_FIELDS where F_INT > CAST(F_DOUBLE AS INT)",
        4,
        EXPECTED6
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Unsupported operation: CAST(class java.lang.Double As Int)"));
  }

  @Test
  public void longGtInt() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_BIGINT > CAST(F_INT AS BigInt)",
      5,
      EXPECTED0
    );
  }

  @Test
  public void longGtLong() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_BIGINT > F_BIGINT",
      0
    );
  }

  @Test
  public void longGtString() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_BIGINT > F_VARCHAR",
      5,
      EXPECTED0
    );
  }

  @Test
  public void longGtTimestamp() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
      "select * from ALL_FIELDS where F_BIGINT > F_TIMESTAMP",
      10
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>' to arguments of type '<BIGINT> > <TIMESTAMP(0)>'"));
  }

  @Test
  public void longGtTime() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_BIGINT > F_TIME",
       10
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>' to arguments of type '<BIGINT> > <TIME(0)>'"));
  }

  @Test
  public void longGtDate() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_BIGINT > F_DATE",
       10
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>' to arguments of type '<BIGINT> > <DATE>'"));
  }

  @Test
  public void longGtDouble() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where CAST(F_BIGINT AS DOUBLE) > F_DOUBLE",
      0
    );
  }

  @Test
  public void longGtDouble2() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_BIGINT > CAST(F_DOUBLE AS BIGINT)",
       4,
       EXPECTED6
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Unsupported operation: CAST(class java.lang.Double As Long)"));
  }

  @Test
  public void stringGtInt() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_VARCHAR > F_INT",
      0
    );
  }

  @Test
  public void stringGtLong() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_VARCHAR > F_BIGINT",
      4,
      EXPECTED6
    );
  }

  @Test
  public void stringGtString() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_VARCHAR > F_VARCHAR",
      0
    );
  }

  @Test
  public void stringGtTimestamp() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_VARCHAR > F_TIMESTAMP",
       10
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Unsupported operation: CAST(class org.apache.arrow.vector.util.Text As Timestamp)"));
  }

  @Test
  public void stringGtTime() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_VARCHAR > F_TIME",
       10
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Unsupported operation: CAST(class org.apache.arrow.vector.util.Text As TIME)"));
  }

  @Test
  public void stringGtDate() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
      "select * from ALL_FIELDS where F_VARCHAR > F_DATE",
      10
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Unsupported operation: CAST(class org.apache.arrow.vector.util.Text As DATE)"));
  }

  @Test
  public void stringGtDouble() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_VARCHAR > F_DOUBLE",
      4,
      EXPECTED6
    );
  }

  @Test
  public void timestampGtInt() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIMESTAMP > F_INT",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>' to arguments of type '<TIMESTAMP(0)> > <INTEGER>'"));
  }

  @Test
  public void timestampGtLong() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIMESTAMP > F_BIGINT",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>' to arguments of type '<TIMESTAMP(0)> > <BIGINT>'"));
  }

  @Test
  public void timestampGtString() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIMESTAMP > F_VARCHAR",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Unsupported operation: CAST(class org.apache.arrow.vector.util.Text As Timestamp)"));
  }

  @Test
  public void timestampGtTimestamp() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_TIMESTAMP > F_TIMESTAMP",
      0
    );
  }

  @Test
  public void timestampGtTime() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIMESTAMP > F_TIME",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>' to arguments of type '<TIMESTAMP(0)> > <TIME(0)>'"));
  }

  @Test
  public void timestampGtDate() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_TIMESTAMP > F_DATE",
      0
    );
  }

  @Test
  public void timestampGtDouble() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIMESTAMP > F_DOUBLE",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>' to arguments of type '<TIMESTAMP(0)> > <DOUBLE>'"));
  }

  @Test
  public void timeGtInt() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIME > F_INT",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>' to arguments of type '<TIME(0)> > <INTEGER>'"));
  }

  @Test
  public void timeGtLong() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIME > F_BIGINT",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>' to arguments of type '<TIME(0)> > <BIGINT>'"));
  }

  @Test
  public void timeGtString() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIME > F_VARCHAR",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Unsupported operation: CAST(class org.apache.arrow.vector.util.Text As TIME)"));
  }

  @Test
  public void timeGtTimestamp() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIME > F_TIMESTAMP",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>' to arguments of type '<TIME(0)> > <TIMESTAMP(0)>'"));
  }

  @Test
  public void timeGtTime() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_TIME > F_TIME",
      0
    );
  }

  @Test
  public void timeGtDate() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_TIME > F_DATE",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>' to arguments of type '<TIME(0)> > <DATE>'"));
  }

  @Test
  public void timeGtDouble() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
        "select * from ALL_FIELDS where F_TIME > F_DOUBLE",
        0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>' to arguments of type '<TIME(0)> > <DOUBLE>'"));
  }

  @Test
  public void dateGtInt() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_DATE > F_INT",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>' to arguments of type '<DATE> > <INTEGER>'"));
  }

  @Test
  public void dateGtLong() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_DATE > F_BIGINT",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>' to arguments of type '<DATE> > <BIGINT>'"));
  }

  @Test
  public void dateGtString() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_DATE > F_VARCHAR",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Unsupported operation: CAST(class org.apache.arrow.vector.util.Text As DATE)"));
  }

  @Test
  public void dateGtTimestamp() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_DATE > F_TIMESTAMP",
      0
    );
  }

  @Test
  public void dateGtTime() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
        "select * from ALL_FIELDS where F_DATE > F_TIME",
        0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>' to arguments of type '<DATE> > <TIME(0)>'"));
  }

  @Test
  public void dateGtDate() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_DATE > F_DATE",
      0
    );
  }

  @Test
  public void dateGtDouble() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_DATE > F_DOUBLE",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>' to arguments of type '<DATE> > <DOUBLE>'"));
  }

  @Test
  public void doubleGtInt() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_DOUBLE > CAST(F_INT AS Double)",
      5,
      EXPECTED0
    );
  }

  @Test
  public void doubleGtLong() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_DOUBLE > CAST(F_BIGINT AS Double)",
      0
    );
  }

  @Test
  public void doubleGtString() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_DOUBLE > F_VARCHAR",
      5,
      EXPECTED0
    );
  }

  @Test
  public void doubleGtTimestamp() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_DOUBLE > F_TIMESTAMP",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>' to arguments of type '<DOUBLE> > <TIMESTAMP(0)>'"));
  }

  @Test
  public void doubleGtTime() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_DOUBLE > F_TIME",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>' to arguments of type '<DOUBLE> > <TIME(0)>'"));
  }

  @Test
  public void doubleGtDate() {
    Throwable e = assertThrows(SQLException.class, () ->
      TestUtils.filterTest(
       "select * from ALL_FIELDS where F_DOUBLE > F_DATE",
       0
      )
    );
    assertThat(e.getCause().getMessage(),
      containsString("Cannot apply '>' to arguments of type '<DOUBLE> > <DATE>'"));
  }

  @Test
  public void doubleGtDouble() throws SQLException {
    TestUtils.filterTest(
      "select * from ALL_FIELDS where F_DOUBLE > F_DOUBLE",
      0
    );
  }
}
