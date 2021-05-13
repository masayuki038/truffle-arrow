package net.wrap_trap.truffle_arrow.language.truffle.node;

import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.NodeField;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.frame.VirtualFrame;

/**
 * Writes a local variable.
 * Based on SLWriteLocalVariableNode
 */
@NodeChild(value = "valueNode", type = ExprBase.class)
@NodeField(name = "slot", type = FrameSlot.class)
public abstract class StatementWriteLocal extends StatementBase {

  protected abstract FrameSlot getSlot();

  @Specialization(guards = "isIntOrIllegal(frame)")
  protected void writeInt(VirtualFrame frame, int value) {
    // Initialize type on first write of the local variable. No-op if kind is already Boolean.
    getSlot().setKind(FrameSlotKind.Int);

    frame.setInt(getSlot(), value);
  }

  protected boolean isIntOrIllegal(VirtualFrame frame) {
    return getSlot().getKind() == FrameSlotKind.Int || getSlot().getKind() == FrameSlotKind.Illegal;
  }

  @Specialization(guards = "isBooleanOrIllegal(frame)")
  protected void writeBoolean(VirtualFrame frame, boolean value) {
    // Initialize type on first write of the local variable. No-op if kind is already Boolean.
    getSlot().setKind(FrameSlotKind.Boolean);

    frame.setBoolean(getSlot(), value);
  }

  /**
   * @param frame The parameter seems unnecessary, but it is required: Without the parameter, the
   *            Truffle DSL would not check the guard on every execution of the specialization.
   *            Guards without parameters are assumed to be pure, but our guard depends on the
   *            slot kind which can change.
   */
  protected boolean isBooleanOrIllegal(VirtualFrame frame) {
    return getSlot().getKind() == FrameSlotKind.Boolean || getSlot().getKind() == FrameSlotKind.Illegal;
  }

  @Specialization(guards = "isLongOrIllegal(frame)")
  protected void writeLong(VirtualFrame frame, long value) {
    // Initialize type on first write of the local variable. No-op if kind is already Long.
    getSlot().setKind(FrameSlotKind.Long);

    frame.setLong(getSlot(), value);
  }

  /**
   * @param frame The parameter seems unnecessary, but it is required: Without the parameter, the
   *            Truffle DSL would not check the guard on every execution of the specialization.
   *            Guards without parameters are assumed to be pure, but our guard depends on the
   *            slot kind which can change.
   */
  protected boolean isLongOrIllegal(VirtualFrame frame) {
    return getSlot().getKind() == FrameSlotKind.Long || getSlot().getKind() == FrameSlotKind.Illegal;
  }

  @Specialization(guards = "isDoubleOrIllegal(frame)")
  protected void writeDouble(VirtualFrame frame, double value) {
    // Initialize type on first write of the local variable. No-op if kind is already Double.
    getSlot().setKind(FrameSlotKind.Double);

    frame.setDouble(getSlot(), value);
  }

  /**
   * @param frame The parameter seems unnecessary, but it is required: Without the parameter, the
   *            Truffle DSL would not check the guard on every execution of the specialization.
   *            Guards without parameters are assumed to be pure, but our guard depends on the
   *            slot kind which can change.
   */
  protected boolean isDoubleOrIllegal(VirtualFrame frame) {
    return getSlot().getKind() == FrameSlotKind.Double || getSlot().getKind() == FrameSlotKind.Illegal;
  }

  @Specialization
  protected void write(VirtualFrame frame, Object value) {
    /*
     * Regardless of the type before, the new and final type of the local variable is Object.
     * Changing the slot kind also discards compiled code, because the variable type is
     * important when the compiler optimizes a method.
     *
     * No-op if kind is already Object.
     */
    getSlot().setKind(FrameSlotKind.Object);

    frame.setObject(getSlot(), value);
  }
}
