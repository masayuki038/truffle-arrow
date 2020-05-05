package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.nodes.NodeInfo;

import net.wrap_trap.truffle_arrow.ArrowUtils;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.util.Text;

import java.time.Instant;

@NodeInfo(shortName = "=")
abstract class ExprFilter extends ExprBinary {

  @Specialization
  protected UInt4Vector filter(VarCharVector left, String right) {
    return filter(left, new Text(right));
  }

  @Specialization
  protected UInt4Vector filter(String left, VarCharVector right) {
    return filter(right, new Text(left));
  }

  @Specialization
  protected UInt4Vector filter(IntVector left, Long right) {
    return filter(left, right.intValue());
  }

  @Specialization
  protected UInt4Vector filter(Long left, IntVector right) {
    return filter(right, left.intValue());
  }

  @Specialization
  protected UInt4Vector filter(BigIntVector left, Integer right) {
    return filter(left, right.longValue());
  }

  @Specialization
  protected UInt4Vector filter(Integer left, BigIntVector right) {
    return filter(right, left.longValue());
  }

  @Specialization
  protected UInt4Vector filter(TimeStampSecTZVector left, Instant right) {
    return filter(left, right.toEpochMilli());
  }

  @Specialization
  protected UInt4Vector filter(Instant left, TimeStampSecTZVector right) {
    return filter(right, left.toEpochMilli());
  }

  @Specialization
  protected UInt4Vector filter(FieldVector left, Object right) {
    UInt4Vector selectionVector = ArrowUtils.createSelectionVector();
    int selectionIndex = 0;

    selectionVector.setValueCount(left.getValueCount());
    for (int i = 0; i < left.getValueCount(); i++) {
      if (left.getObject(i).equals(right)) {
        selectionVector.set(selectionIndex ++, i);
      }
    }
    selectionVector.setValueCount(selectionIndex);
    return selectionVector;
  }

  @Specialization
  protected UInt4Vector filter(Object left, FieldVector right) {
    return filter(right, left);
  }
}
