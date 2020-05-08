package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.nodes.NodeInfo;

import net.wrap_trap.truffle_arrow.ArrowUtils;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.util.Text;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;

@NodeInfo(shortName = "=")
abstract class ExprFilter extends ExprBinary {

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

  @Specialization
  protected UInt4Vector filter(FieldVector left, Object right) {
    return eval(left, right, false);
  }

  @Specialization
  protected UInt4Vector filter(Object left, FieldVector right) {
    return filter(right, left);
  }

  protected UInt4Vector eval(FieldVector left, Object right, boolean reverse) {
    UInt4Vector selectionVector = ArrowUtils.createSelectionVector();
    int selectionIndex = 0;

    selectionVector.setValueCount(left.getValueCount());
    for (int i = 0; i < left.getValueCount(); i++) {
      Object o = left.getObject(i);
      if (o instanceof Text) {
        o = ((Text) o).toString();
      }
      if (compare((Comparable) o, right, reverse)) {
        selectionVector.set(selectionIndex ++, i);
      }
    }
    selectionVector.setValueCount(selectionIndex);
    return selectionVector;
  }

  protected boolean compare(Comparable left, Object right, boolean ignore) {
    return left.compareTo(right) == 0;
  }
}
