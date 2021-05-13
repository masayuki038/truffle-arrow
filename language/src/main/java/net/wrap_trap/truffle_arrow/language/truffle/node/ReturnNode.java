package net.wrap_trap.truffle_arrow.language.truffle.node;

import com.oracle.truffle.api.frame.VirtualFrame;

public class ReturnNode extends StatementBase {
  @Child private ExprBase result;

  public ReturnNode(ExprBase result) {
    this.result = result;
  }

  @Override
  public void executeVoid(VirtualFrame virtualFrame) {
    Object value = result.executeGeneric(virtualFrame);
    throw new ReturnException(value);
  }
}