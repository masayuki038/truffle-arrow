package net.wrap_trap.truffle_arrow.language.truffle.node;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;

public class ExprDoubleNode extends ExprBase {

  final double value;

  public ExprDoubleNode(double value) {
    this.value = value;
  }

  @Override
  double executeDouble(VirtualFrame vf) throws UnexpectedResultException {
    return value;
  }

  @Override
  public Object executeGeneric(VirtualFrame virtualFrame) {
    return value;
  }

  @Override
  void executeVoid(VirtualFrame frame) { }

  @Override
  public String toString() {
    return "" + value;
  }
}
