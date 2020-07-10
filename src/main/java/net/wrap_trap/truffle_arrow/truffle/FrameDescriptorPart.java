package net.wrap_trap.truffle_arrow.truffle;

import com.google.common.collect.Lists;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Represents a contiguous subset of frame slots that we use to store a relation
 */
public class FrameDescriptorPart {
  private final FrameDescriptor frame;
  private final int startOffset;
  private int size;
  private FrameDescriptorPart previous;

  private FrameDescriptorPart(FrameDescriptor frame, int startOffset, int size, FrameDescriptorPart previous) {
    this.frame = frame;
    this.startOffset = startOffset;
    this.size = size;
    this.previous = previous;
  }

  /**
   * The underyling frame, which includes all the slots
   */
  FrameDescriptor frame() {
    return frame;
  }

  /**
   * The number of slots we are using
   */
  public int size() {
    return size;
  }

  public List<FrameSlot> getFrameSlots() {
    List<FrameSlot> frameSlotList = Lists.newArrayList();
    for (int i = this.startOffset; i < this.startOffset + this.size; i ++) {
      frameSlotList.add(this.frame.findFrameSlot(i));
    }
    return frameSlotList;
  }

  static FrameDescriptorPart root(int slots) {
    FrameDescriptor frame = new FrameDescriptor();

    for (int i = 0; i < slots; i++)
      frame.addFrameSlot(i);

    return new FrameDescriptorPart(frame, 0, slots, null);
  }

  FrameDescriptorPart newPart() {
    return new FrameDescriptorPart(frame, startOffset + size, 0, this);
  }

  FrameSlot addFrameSlot() {
    return frame.addFrameSlot(startOffset + this.size ++);
  }

  int getCurrentSlotPosition() {
    return this.size - 1;
  }

  FrameSlot findFrameSlotInPrevious(int index) {
    if (this.previous == null) {
      new IllegalStateException("Previous FrameDescriptorPart is null");
    }
    return this.previous.findFrameSlot(index);
  }

  FrameSlot findFrameSlot(int index) {
    return frame().findFrameSlot(startOffset + index);
  }

  void setFrameSlotKind(FrameSlot slot, FrameSlotKind kind) {
    frame().setFrameSlotKind(slot, kind);
  }
}
