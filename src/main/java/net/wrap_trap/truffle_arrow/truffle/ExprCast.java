package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.frame.FrameSlotKind;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.arrow.vector.util.Text;

import static sun.security.krb5.Confounder.longValue;

@NodeChild("target")
abstract public class ExprCast extends ExprBase {
  private final RelDataType type;

  protected ExprCast(RelDataType type) {
    this.type = type;
  }

  @Specialization(guards = "asInt()")
  protected Object castToInt(Object value) {
    if (value instanceof Integer) {
      return value;
    } else if (value instanceof Long) {
      Long l = (Long) value;
      if (l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE) {
        return l.intValue();
      }
      throw new IllegalArgumentException("Couldn't cast long value to int: " + value);
    } else if (value instanceof Text) {
      return Integer.parseInt(value.toString());
    } else if (value instanceof String) {
      return Integer.parseInt((String) value);
    }
    throw new UnsupportedOperationException(
      String.format("Unsupported operation: CAST(%s As Int)", value.getClass()));
  }

  @Specialization(guards = "asLong()")
  protected Object castToLong(Object value) {
    if (value instanceof Integer) {
      return ((Integer) value).longValue();
    } else if (value instanceof Long) {
      return value;
    } else if (value instanceof Text) {
      return Long.parseLong(value.toString());
    }
    throw new UnsupportedOperationException(
      String.format("Unsupported operation: CAST(%s As Long)", value.getClass()));
  }

  @Specialization(guards = "asDouble()")
  protected Object castToDouble(Object value) {
    if (value instanceof Integer) {
      return ((Integer) value).doubleValue();
    } else if (value instanceof Long) {
      return ((Long) value).doubleValue();
    } else if (value instanceof Text) {
      return Double.parseDouble(value.toString());
    }
    throw new UnsupportedOperationException(
      String.format("Unsupported operation: CAST(%s As Long)", value.getClass()));
  }

  @Specialization(guards = "asString()")
  protected Object castToString(Object value) {
    if (value instanceof Integer) {
      return value.toString();
    } else if (value instanceof Long) {
      return value.toString();
    } else if (value instanceof Double) {
      return value.toString();
    }
    throw new UnsupportedOperationException(
      String.format("Unsupported operation: CAST(%s As Long)", value.getClass()));
  }

  @Specialization(guards = "asInstant()")
  protected Object castToTimestamp(Object value) {
    if (value instanceof Integer) { // date
      return ((Integer) value) * 24 * 60 * 60 * 1000L;
    }
    throw new UnsupportedOperationException(
      String.format("Unsupported operation: CAST(%s As Timestamp)", value.getClass()));
  }

  @Specialization
  protected Object cast(Object value) {
    throw new UnsupportedOperationException(
      String.format("Unsupported operation: CAST(%s As %s)", value.getClass(), type.getSqlTypeName()));
  }

  protected boolean asBoolean() {
    return Types.kind(type.getSqlTypeName()) == FrameSlotKind.Boolean;
  }

  protected boolean asInt() {
    return Types.kind(type.getSqlTypeName()) == FrameSlotKind.Int;
  }

  protected boolean asLong() {
    return Types.kind(type.getSqlTypeName()) == FrameSlotKind.Long;
  }

  protected boolean asDouble() {
    return Types.kind(type.getSqlTypeName()) == FrameSlotKind.Double;
  }

  protected boolean asLocalDate() {
    return type.getSqlTypeName() == SqlTypeName.DATE;
  }

  protected boolean asInstant() {
    return type.getSqlTypeName() == SqlTypeName.TIMESTAMP;
  }

  protected boolean asString() {
    return type.getSqlTypeName() == SqlTypeName.VARCHAR;
  }
}
