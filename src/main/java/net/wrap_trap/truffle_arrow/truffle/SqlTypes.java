package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.dsl.TypeCast;
import com.oracle.truffle.api.dsl.TypeCheck;
import com.oracle.truffle.api.dsl.TypeSystem;

import java.time.Instant;
import java.time.LocalDate;

@TypeSystem({boolean.class, long.class, double.class, LocalDate.class, Instant.class, String.class, SqlNull.class})
class SqlTypes {

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