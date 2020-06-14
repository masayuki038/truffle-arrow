package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.nodes.NodeInfo;

@NodeInfo(shortName = "OR")
abstract class ExprOr extends ExprBinary {

  @Specialization
  protected boolean or(boolean left, boolean right) {
    return left || right;
  }
}