package net.wrap_trap.truffle_arrow.parallel;

import net.wrap_trap.truffle_arrow.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.*;
import java.util.Comparator;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class AggregateCountTest {

  @BeforeAll
  public static void setupOnce() throws ClassNotFoundException, IOException {
    Class.forName("net.wrap_trap.truffle_arrow.TruffleDriver");
    TestUtils.generateTestFiles("target/classes/samples/files/all_fields", "202009", TestDataType.CASE1);
    TestUtils.generateTestFiles("target/classes/samples/files/all_fields", "202010", TestDataType.CASE2);
    TestUtils.generateTestFiles("target/classes/samples/files/all_fields", "202011", TestDataType.CASE3);
    TestUtils.generateTestFiles("target/classes/samples/files/all_fields", "202012", TestDataType.CASE4);
    TestUtils.generateTestFiles("target/classes/samples/files/all_fields", "202101", TestDataType.CASE5);
    TestUtils.reloadTruffleArrowConfig();
  }

  @AfterAll
  public static void teardownOnce() throws IOException {
    TestUtils.deleteDirectory("target/classes/samples/files/all_fields");
  }

  @Test
  public void countByInt() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_INT, COUNT(F_INT) AS CNT from ALL_FIELDS group by F_INT");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(10));
      results.sort(Comparator.naturalOrder());
      assertThat(results.get(0), is("0\t5"));
      assertThat(results.get(1), is("1\t5"));
      assertThat(results.get(2), is("2\t5"));
      assertThat(results.get(3), is("3\t5"));
      assertThat(results.get(4), is("4\t5"));
      assertThat(results.get(5), is("5\t5"));
      assertThat(results.get(6), is("6\t5"));
      assertThat(results.get(7), is("7\t5"));
      assertThat(results.get(8), is("8\t5"));
      assertThat(results.get(9), is("9\t5"));
      TestUtils.assertRelInclude(ArrowAggregate.class);
    }
  }
}