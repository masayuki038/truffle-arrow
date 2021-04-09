package net.wrap_trap.truffle_arrow;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;

public class SqlParser {

  public static SqlNode parse(String sql) {
    try {
      org.apache.calcite.sql.parser.SqlParser.Config config =
        org.apache.calcite.sql.parser.SqlParser.configBuilder().setLex(Lex.JAVA).build();
      org.apache.calcite.sql.parser.SqlParser parser = org.apache.calcite.sql.parser.SqlParser.create(sql, config);
      return parser.parseStmt();
    } catch (SqlParseException e) {
      throw new RuntimeException(e);
    }
  }
}
