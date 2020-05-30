package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.time.Instant;
import java.time.LocalDate;

@NodeChild("target")
abstract public class ExprCast extends ExprBase {
  private final RelDataType type;

  protected ExprCast(RelDataType type) {
    this.type = type;
  }

  @Specialization(guards = "asBoolean()")
  protected boolean executeBoolean(boolean value) {
    return value;
  }

  @Specialization(guards = "asLong()")
  protected long executeLong(long value) {
    return value;
  }

  @Specialization(guards = "asLong()")
  protected long executeLong(double value) {
    return (long) value;
  }

  @Specialization(guards = "asDouble()")
  protected double executeDouble(double value) {
    return value;
  }

  @Specialization(guards = "asDouble()")
  protected double executeDouble(long value) {
    return value;
  }

  @Specialization(guards = "asLocalDate()")
  protected LocalDate executeLocalDate(LocalDate value) {
    return value;
  }

  @Specialization(guards = "asInstant()")
  protected Instant executeInstant(Instant value) {
    return value;
  }

  @Specialization(guards = "asString()")
  protected Object executeString(Object value) {
    if (value == SqlNull.INSTANCE)
      return SqlNull.INSTANCE;
    else
      return String.valueOf(value);
  }

  @Specialization
  protected FieldVector executeIntVector(final IntVector vector) {
    FrameSlotKind frameSlotKind = Types.kind(type.getSqlTypeName());
    switch(frameSlotKind) {
      case Long:
        return new FieldVectorProxy(vector) {
          @Override
          public Object getObject(int i) {
            return vector.getObject(i).longValue();
          }
        };
      case Double:
        return new FieldVectorProxy(vector) {
          @Override
          public Object getObject(int i) {
            return vector.getObject(i).doubleValue();
          }
        };
      default:
        throw new UnsupportedOperationException(
          String.format("Unsupported operation: CAST(Int As %s)", frameSlotKind));
    }
  }

  @Specialization
  protected FieldVector executeLongVector(final BigIntVector vector) {
    FrameSlotKind frameSlotKind = Types.kind(type.getSqlTypeName());
    switch(frameSlotKind) {
      case Double:
        return new FieldVectorProxy(vector) {
          @Override
          public Object getObject(int i) {
            return vector.getObject(i).doubleValue();
          }
        };
      default:
        throw new UnsupportedOperationException(
          String.format("Unsupported operation: CAST(BigInt As %s)", frameSlotKind));
    }
  }

  @Specialization
  protected FieldVector executeVarCharVector(final VarCharVector vector) {
    FrameSlotKind frameSlotKind = Types.kind(type.getSqlTypeName());
    switch(frameSlotKind) {
      case Int:
        return new FieldVectorProxy(vector) {
          @Override
          public Object getObject(int i) {
            return Integer.parseInt(vector.getObject(i).toString());
          }
        };
      case Long:
        return new FieldVectorProxy(vector) {
          @Override
          public Object getObject(int i) {
            return Long.parseLong(vector.getObject(i).toString());
          }
        };
      case Double:
        return new FieldVectorProxy(vector) {
          @Override
          public Object getObject(int i) {
            return Double.parseDouble(vector.getObject(i).toString());
          }
        };
      default:
        throw new UnsupportedOperationException(
          String.format("Unsupported operation: CAST(VarChar As %s)", frameSlotKind));
    }
  }

  @Specialization
  protected FieldVector executeDateDayVector(final DateDayVector vector) {
    FrameSlotKind frameSlotKind = Types.kind(type.getSqlTypeName());
    if (frameSlotKind == FrameSlotKind.Object) { // timestamp
      return new FieldVectorProxy(vector) {
        @Override
        public Long getObject(int i) {
          return vector.getObject(i) * 24 * 60 * 60 * 1000L;
        }
      };
    }
    throw new UnsupportedOperationException(
      String.format("Unsupported operation: CAST(Date As %s)", frameSlotKind));
  }

  @Specialization
  protected FieldVector executeFloat8Vector(final Float8Vector vector) {
    FrameSlotKind frameSlotKind = Types.kind(type.getSqlTypeName());
    throw new UnsupportedOperationException(
      String.format("Unsupported operation: CAST(Float8 As %s)", frameSlotKind));
  }

  @Specialization
  protected SqlNull executeNull(Object any) {
    return SqlNull.INSTANCE;
  }

  protected boolean asBoolean() {
    return Types.kind(type.getSqlTypeName()) == FrameSlotKind.Boolean;
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
