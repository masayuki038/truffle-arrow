package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.nodes.NodeInfo;

@NodeInfo(shortName = "==")
public abstract class ExprCondition extends ExprBinary {
  @Specialization(rewriteOn = ArithmeticException.class)
  protected boolean eq(long left, long right) {
    return left == right;
  }
}
