package net.wrap_trap.truffle_arrow;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class GroupByTest {

  @BeforeAll
  public static void setupOnce() throws ClassNotFoundException, IOException {
    Class.forName("net.wrap_trap.truffle_arrow.TruffleDriver");
    TestUtils.generateTestFile("target/classes/samples/files/all_fields.arrow");
    TruffleArrowConfig.INSTANCE.reload();
  }

  @AfterAll
  public static void teardownOnce() {
    new File("target/classes/samples/files/all_fields.arrow").delete();
  }

  @Test
  public void groupByInt() throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(
        "select F_INT+1, F_VARCHAR from ALL_FIELDS group by F_INT+1, F_VARCHAR");
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(10));
      assertThat(results.get(0), is("0"));
      assertThat(LastPlan.INSTANCE.includes(ArrowAggregate.class), is(true));
    }
  }
}
