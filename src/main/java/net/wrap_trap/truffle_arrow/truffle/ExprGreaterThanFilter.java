package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.nodes.NodeInfo;
import net.wrap_trap.truffle_arrow.ArrowUtils;
import org.apache.arrow.vector.*;

@NodeInfo(shortName = ">")
abstract public class ExprGreaterThanFilter extends ExprFilter {

  @Override
  protected boolean compare(Comparable left, Object right, boolean reverse) {
    if (reverse) {
      return left.compareTo(right) < 0;
    }
    return left.compareTo(right) > 0;
  }
}
