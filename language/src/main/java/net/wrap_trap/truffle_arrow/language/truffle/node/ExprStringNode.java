package net.wrap_trap.truffle_arrow.language.truffle.node;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;

public class ExprStringNode extends ExprBase {

  final String value;

  public ExprStringNode(String value) {
    this.value = value;
  }

  @Override
  String executeString(VirtualFrame vf) throws UnexpectedResultException {
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

