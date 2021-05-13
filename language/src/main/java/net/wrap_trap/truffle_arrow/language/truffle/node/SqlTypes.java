package net.wrap_trap.truffle_arrow.language.truffle.node;

import com.oracle.truffle.api.dsl.TypeCast;
import com.oracle.truffle.api.dsl.TypeCheck;
import com.oracle.truffle.api.dsl.TypeSystem;
import net.wrap_trap.truffle_arrow.language.truffle.node.type.ArrowTimeSec;

import java.time.LocalDate;

@TypeSystem({boolean.class, int.class, long.class, double.class, ArrowTimeSec.class, LocalDate.class, String.class, SqlNull.class})
public class SqlTypes {

  @TypeCheck(SqlNull.class)
  public static boolean isSqlNull(Object value) {
    return value == SqlNull.INSTANCE;
  }

  @TypeCast(SqlNull.class)
  public static SqlNull asSqlNull(Object value) {
    assert isSqlNull(value);

    return SqlNull.INSTANCE;
  }
}
