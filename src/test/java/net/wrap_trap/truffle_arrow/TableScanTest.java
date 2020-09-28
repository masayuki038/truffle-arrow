package net.wrap_trap.truffle_arrow;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.List;

import net.wrap_trap.truffle_arrow.storage.columnar.ArrowColumnarTableScan;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


public class TableScanTest {

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
  public void scanOnlyRequiredFields() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_INT, F_VARCHAR, F_TIMESTAMP from ALL_FIELDS where F_TIMESTAMP=timestamp'2020-05-04 16:48:11'");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(1));
      assertThat(results.get(0), is("3\ttest3\t2020-05-04 16:48:11.0"));
      assertThat(LastPlan.INSTANCE.includes(ArrowColumnarTableScan.class), is(true));
    }
  }
}
