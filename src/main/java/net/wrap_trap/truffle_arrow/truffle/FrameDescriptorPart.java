package net.wrap_trap.truffle_arrow.truffle;

import com.google.common.collect.Lists;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;

import java.util.List;

/**
 * Represents a contiguous subset of frame slots that we use to store a relation
 */
public class FrameDescriptorPart {
  private final FrameDescriptor frame;
  private final FrameDescriptor previous;
  private int size;

  private FrameDescriptorPart(FrameDescriptor frame, FrameDescriptor previous) {
    this.frame = frame;
    this.previous = previous;
  }

  /**
   * The underyling frame, which includes all the slots
   */
  FrameDescriptor frame() {
    return frame;
  }

  public List<FrameSlot> getFrameSlots() {
    List<FrameSlot> frameSlotList = Lists.newArrayList();
    for (int i = 0; i < this.frame.getSize(); i ++) {
      frameSlotList.add(this.frame.findFrameSlot(i));
    }
    return frameSlotList;
  }

  static FrameDescriptorPart root(int slots) {
    FrameDescriptor newFrame = new FrameDescriptor();

    for (int i = 0; i < slots; i++)
      newFrame.addFrameSlot(i);

    return new FrameDescriptorPart(newFrame, null);
  }

  FrameDescriptorPart newPart() {
    FrameDescriptor newFrame = new FrameDescriptor();
    return new FrameDescriptorPart(newFrame, this.frame);
  }

  FrameSlot addFrameSlot() {
    return frame.addFrameSlot(this.size ++);
  }

  int getCurrentSlotPosition() {
    return this.size - 1;
  }

  FrameSlot findFrameSlotInPrevious(int index) {
    if (this.previous == null) {
      throw new IllegalStateException("Previous FrameDescriptorPart is null");
    }
    return this.previous.findFrameSlot(index);
  }

  FrameSlot findFrameSlot(int index) {
    return frame().findFrameSlot(index);
  }

  void setFrameSlotKind(FrameSlot slot, FrameSlotKind kind) {
    frame().setFrameSlotKind(slot, kind);
  }
}
