package net.wrap_trap.truffle_arrow.language.builtins;

import com.oracle.truffle.api.dsl.UnsupportedSpecializationException;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import net.wrap_trap.truffle_arrow.language.TruffleArrowException;

public abstract class BuiltinNode {
  @Override
  public final Object executeGeneric(VirtualFrame frame) {
    try {
      return execute(frame);
    } catch (UnsupportedSpecializationException e) {
      throw TruffleArrowException.typeError(e.getNode(), e.getSuppliedValues());
    }
  }

  @Override
  public final boolean executeBoolean(VirtualFrame frame) throws UnexpectedResultException {
    return super.executeBoolean(frame);
  }

  @Override
  public final long executeLong(VirtualFrame frame) throws UnexpectedResultException {
    return super.executeLong(frame);
  }

  @Override
  public final void executeVoid(VirtualFrame frame) {
    super.executeVoid(frame);
  }

  protected abstract Object execute(VirtualFrame frame);
}
