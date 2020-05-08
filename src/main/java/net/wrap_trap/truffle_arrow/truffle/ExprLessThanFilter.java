package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.nodes.NodeInfo;
import net.wrap_trap.truffle_arrow.ArrowUtils;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt4Vector;

@NodeInfo(shortName = "<")
abstract public class ExprLessThanFilter extends ExprFilter {

  @Override
  protected boolean compare(Comparable left, Object right, boolean reverse) {
    if (reverse) {
      return left.compareTo(right) > 0;
    }
    return left.compareTo(right) < 0;
  }
}