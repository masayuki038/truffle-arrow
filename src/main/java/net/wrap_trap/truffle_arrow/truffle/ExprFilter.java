package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.nodes.NodeInfo;

import net.wrap_trap.truffle_arrow.ArrowUtils;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.util.Text;

@NodeInfo(shortName = "=")
abstract class ExprFilter extends ExprBinary {

  @Specialization
  protected UInt4Vector filter(FieldVector left,String right) {
    return filter(left, new Text(right));
  }

  @Specialization
  protected UInt4Vector filter(String left, FieldVector right) {
    return filter(right, new Text(left));
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
