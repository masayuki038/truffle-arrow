package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.nodes.NodeInfo;

import net.wrap_trap.truffle_arrow.ArrowUtils;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt4Vector;

@NodeInfo(shortName = "=")
abstract class ExprFilter extends ExprBinary {

  @Specialization
  protected UInt4Vector filter(FieldVector left, long right) {
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
  @CompilerDirectives.TruffleBoundary
  protected boolean eq(Object left, Object right) {
    return left == right;
  }
}
