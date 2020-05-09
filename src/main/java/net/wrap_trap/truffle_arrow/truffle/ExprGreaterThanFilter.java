package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.nodes.NodeInfo;
import org.apache.arrow.vector.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;

@NodeInfo(shortName = ">")
abstract public class ExprGreaterThanFilter extends ExprFilter {

  @Specialization
  protected UInt4Vector filter(IntVector left, Long right) {
    return eval(left, right.intValue(), false);
  }

  @Specialization
  protected UInt4Vector filter(Long left, IntVector right) {
    return eval(right, left.intValue(), true);
  }

  @Specialization
  protected UInt4Vector filter(BigIntVector left, Integer right) {
    return eval(left, right.longValue(), false);
  }

  @Specialization
  protected UInt4Vector filter(Integer left, BigIntVector right) {
    return eval(right, left.longValue(), true);
  }

  @Specialization
  protected UInt4Vector filter(TimeStampSecTZVector left, Instant right) {
    return eval(left, right.toEpochMilli(), false);
  }

  @Specialization
  protected UInt4Vector filter(Instant left, TimeStampSecTZVector right) {
    return eval(right, left.toEpochMilli(), true);
  }

  @Specialization
  protected UInt4Vector filter(TimeSecVector left, LocalTime right) {
    return eval(left, right.toSecondOfDay(), false);
  }

  @Specialization
  protected UInt4Vector filter(LocalTime left, TimeSecVector right) {
    return eval(right, left.toSecondOfDay(), true);
  }

  @Specialization
  protected UInt4Vector filter(DateDayVector left, LocalDate right) {
    return eval(left, Long.valueOf(right.toEpochDay()).intValue(), false);
  }

  @Specialization
  protected UInt4Vector filter(LocalDate left, DateDayVector right) {
    return eval(right, Long.valueOf(left.toEpochDay()).intValue(), true);
  }

  // For double because '123.456' generate the instance of BigDecimal at comparing with '>' by calcite
  // '123.456' generate the instance of Double at comparing with '='
  // So ExprFilter have no methods for Float8Vector
  @Specialization
  protected UInt4Vector filter(Float8Vector left, BigDecimal right) {
    return eval(left, right.doubleValue(), false);
  }

  @Specialization
  protected UInt4Vector filter(BigDecimal left, Float8Vector right) {
    return eval(right, left.doubleValue(), true);
  }

  @Specialization
  protected UInt4Vector filter(FieldVector left, Object right) {
    return eval(left, right, false);
  }

  @Specialization
  protected UInt4Vector filter(Object left, FieldVector right) {
    return eval(right, left, true);
  }

  @Override
  protected boolean compare(Comparable left, Object right, boolean reverse) {
    if (reverse) {
      return left.compareTo(right) < 0;
    }
    return left.compareTo(right) > 0;
  }
}
