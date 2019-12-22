package net.wrap_trap.truffle_arrow;

import org.apache.commons.lang3.tuple.Triple;
import org.junit.Test;

import java.sql.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Query tests for ArrowTable
 */
public class JdbcTest {

  @Test
  public void nationsAll() throws SQLException, ClassNotFoundException {
    Class.forName("net.wrap_trap.truffle_arrow.TruffleDriver");
    try (Connection conn = DriverManager.getConnection("jdbc:truffle://localhost:80")) {
      PreparedStatement pstmt = conn.prepareStatement("select N_NATIONKEY, N_NAME, N_REGIONKEY from NATIONSSF");
      ResultSet rs = pstmt.executeQuery();
      resultSetPrint(rs);
    }
  }


  private static void resultSetPrint(ResultSet rs) throws SQLException {
    ResultSetMetaData meta = rs.getMetaData();
    int count = meta.getColumnCount();
    System.out.println(IntStream.rangeClosed(1, count)
                         .mapToObj(i -> {
                           try {
                             return meta.getColumnLabel(i);
                           } catch (Exception e) {
                             throw new RuntimeException(e);
                           }
                         })
                         .collect(Collectors.joining("\t")));
    while (rs.next()) {
      System.out.println(IntStream.rangeClosed(1, count)
                           .mapToObj(i -> {
                             try {
                               return rs.getObject(i);
                             } catch (Exception e) {
                               throw new RuntimeException(e);
                             }
                           }).map(o -> o == null ? "" : o.toString())
                           .collect(Collectors.joining("\t")));
    }
  }



}
