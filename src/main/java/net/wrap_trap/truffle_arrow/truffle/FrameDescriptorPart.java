package net.wrap_trap.truffle_arrow.truffle;

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

  private FrameDescriptorPart(FrameDescriptor frame, int startOffset, int size) {
    this.frame = frame;
    this.startOffset = startOffset;
    this.size = size;
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
    return frame.getIdentifiers().stream().map(id -> {
      assert id instanceof Integer;
      return this.findFrameSlot((Integer) id);
    }).collect(Collectors.toList());
  }

  static FrameDescriptorPart root(int slots) {
    FrameDescriptor frame = new FrameDescriptor();

    for (int i = 0; i < slots; i++)
      frame.addFrameSlot(i);

    return new FrameDescriptorPart(frame, 0, slots);
  }

  FrameDescriptorPart newPart() {
    return new FrameDescriptorPart(frame, startOffset + size, 0);
  }

  FrameSlot addFrameSlot(int index) {
    frame.addFrameSlot(index);
    this.size ++;
    return findFrameSlot(index);
  }

  FrameSlot findFrameSlot(int index) {
    return frame().findFrameSlot(startOffset + index);
  }

  void setFrameSlotKind(FrameSlot slot, FrameSlotKind kind) {
    frame().setFrameSlotKind(slot, kind);
  }
}
