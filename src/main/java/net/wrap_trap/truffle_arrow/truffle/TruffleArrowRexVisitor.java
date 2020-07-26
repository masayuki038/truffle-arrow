package net.wrap_trap.truffle_arrow.truffle;

import org.apache.calcite.rex.RexVisitor;

public interface TruffleArrowRexVisitor<R> extends RexVisitor<R> {
  R visitFrameSlotRef(RexFrameSlotRef rexFrameSlotRef);
}
