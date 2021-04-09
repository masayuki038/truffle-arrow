package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.FrameSlotKind;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;

import java.time.ZoneOffset;
import java.util.Calendar;

public class Types {

  /**
   * What type of slot do we need to represent a non-nullable value of a type?
   */
  public static FrameSlotKind kind(SqlTypeName type) {
    switch (type) {
      case BOOLEAN:
        return FrameSlotKind.Boolean;
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return FrameSlotKind.Int;
      case BIGINT:
        return FrameSlotKind.Long;
      case FLOAT:
      case REAL:
      case DOUBLE:
        return FrameSlotKind.Double;
      case DECIMAL:
      default:
        return FrameSlotKind.Object;
    }
  }

  /**
   * Convert a SQL literal to a runtime value
   */
  public static Object coerceLiteral(RexLiteral literal) {
    if (RexLiteral.isNullLiteral(literal))
      return SqlNull.INSTANCE;

    Object value = literal.getValue();
    RelDataType type = literal.getType();

    return coerceAny(value, type);
  }

  /**
   * Coerce value from any reasonable representation to our internal representation of type.
   * Not fast! Suitable for things like literals, mocks that are executed infrequently.
   */
  public static Object coerceAny(Object value, RelDataType type) {
    if (value == null)
      return SqlNull.INSTANCE;

    switch (type.getSqlTypeName()) {
      case BOOLEAN:
        return value;
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return ((Number) value).intValue();
      case BIGINT:
        return ((Number) value).longValue();
      case DECIMAL:
        assert value instanceof java.math.BigDecimal;
        return value;
      case FLOAT:
      case REAL:
      case DOUBLE:
        return ((Number) value).doubleValue();
      case DATE:
        return ((Calendar) value).toInstant().atOffset(ZoneOffset.UTC).toLocalDate();
      case TIME:
        return ((Calendar) value).toInstant().atOffset(ZoneOffset.UTC).toLocalTime();
      case TIMESTAMP:
        return ((Calendar) value).toInstant();
      case INTERVAL_YEAR:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_MONTH:
      case INTERVAL_DAY:
      case INTERVAL_DAY_HOUR:
      case INTERVAL_DAY_MINUTE:
      case INTERVAL_DAY_SECOND:
      case INTERVAL_HOUR:
      case INTERVAL_HOUR_MINUTE:
      case INTERVAL_HOUR_SECOND:
      case INTERVAL_MINUTE:
      case INTERVAL_MINUTE_SECOND:
      case INTERVAL_SECOND:
        throw new UnsupportedOperationException();
      case CHAR:
      case VARCHAR:
        if (value instanceof NlsString)
          return ((NlsString) value).getValue();
        else
          return (String) value;
      case BINARY:
      case VARBINARY:
        throw new UnsupportedOperationException();
      case NULL:
        return SqlNull.INSTANCE;
      case ANY:
        return value;
      case SYMBOL:
      case MULTISET:
      case ARRAY:
      case MAP:
      case DISTINCT:
      case STRUCTURED:
      case ROW:
      case OTHER:
      case CURSOR:
      case COLUMN_LIST:
      case DYNAMIC_STAR:
      default:
        throw new UnsupportedOperationException();
    }
  }
}
