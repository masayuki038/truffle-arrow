package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.dsl.NodeField;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.frame.FrameUtil;
import com.oracle.truffle.api.frame.VirtualFrame;

import java.util.List;

/**
 * Read a local array variable and return the element specified by index
 * Based on SLReadLocalVariableNode
 */
@NodeField(name = "slot", type = FrameSlot.class)
@NodeField(name = "index", type = Integer.class)
abstract class ExprReadLocalArray extends ExprBase {
  protected abstract FrameSlot getSlot();
  protected abstract int getIndex();

  @Specialization(guards = "isObject(frame)")
  protected Object readObject(VirtualFrame frame) {
    Object result = FrameUtil.getObjectSafe(frame, getSlot());
    assert(result instanceof List);

    return ((List<Object>) result).get(getIndex());
  }

  protected boolean isObject(VirtualFrame frame) {
    return getSlot().getKind() == FrameSlotKind.Object;
  }
}