package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.calcite.rex.RexNode;


public class FilterSink extends RowSink {

  public static FilterSink createSink(FrameDescriptor frameDescriptor, RexNode condition, ThenRowSink next) {
    RowSink rowSink = next.apply(frameDescriptor);
    return new FilterSink(CompileExpr.compile(frameDescriptor, condition), rowSink);
  }

  RowSink then;
  ExprBase conditionExpr;

  private FilterSink(ExprBase conditionExpr, RowSink then) {
    this.conditionExpr = conditionExpr;
    this.then = then;
  }

  @Override
  public void executeVoid(VirtualFrame frame, FrameDescriptor frameDescriptor) throws UnexpectedResultException {
    UInt4Vector selectionVector = (UInt4Vector) conditionExpr.executeGeneric(frame);
    FrameSlot slot1 = frameDescriptor.findFrameSlot(1);
    if (slot1 == null) {
      slot1 = frameDescriptor.addFrameSlot(1);
    }
    frame.setObject(slot1, selectionVector);
    then.executeVoid(frame, frameDescriptor);
  }
}
