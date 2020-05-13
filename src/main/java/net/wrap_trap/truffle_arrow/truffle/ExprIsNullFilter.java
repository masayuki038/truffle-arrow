package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.Specialization;

import net.wrap_trap.truffle_arrow.ArrowUtils;
import org.apache.arrow.vector.*;

@NodeChild("target")
abstract class ExprIsNullFilter extends ExprBase {
  @Specialization
  protected UInt4Vector eval(FieldVector left) {
    UInt4Vector selectionVector = ArrowUtils.createSelectionVector();
    int selectionIndex = 0;

    selectionVector.setValueCount(left.getValueCount());
    for (int i = 0; i < left.getValueCount(); i++) {
      if (left.isNull(i)) {
        selectionVector.set(selectionIndex++, i);
      }
    }
    selectionVector.setValueCount(selectionIndex);
    return selectionVector;
  }
}