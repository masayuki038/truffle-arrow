package net.wrap_trap.truffle_arrow.truffle;

import com.google.common.collect.Lists;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import org.apache.calcite.rel.type.RelDataType;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a contiguous subset of frame slots that we use to store a relation.
 *
 * If thinking about refactoring this, remember that it should be processed all computations
 * by one frame because it needs to move local values from previous FrameDescriptorPart to current,
 * especially SQL Functions (ex. max)
 */
public class FrameDescriptorPart {
  private final FrameDescriptor frame;
  private final int startOffset;
  private int size;
  private FrameDescriptorPart previous;
  private List<RelDataType> relDataTypes = new ArrayList<>();

  private FrameDescriptorPart(FrameDescriptor frame, int startOffset, int size, FrameDescriptorPart previous) {
    this.frame = frame;
    this.startOffset = startOffset;
    this.size = size;
    this.previous = previous;
  }

  /**
   * The underlying frame, which includes all the slots
   */
  public FrameDescriptor frame() {
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

  public static FrameDescriptorPart root(int slots) {
    FrameDescriptor frame = new FrameDescriptor();

    for (int i = 0; i < slots; i++)
      frame.addFrameSlot(i);

    return new FrameDescriptorPart(frame, 0, slots, null);
  }

  public FrameDescriptorPart newPart() {
    return new FrameDescriptorPart(frame, startOffset + size, 0, this);
  }

  public FrameSlot addFrameSlot() {
    return frame.addFrameSlot(startOffset + this.size ++);
  }

  public int getCurrentSlotPosition() {
    return this.size - 1;
  }

  public FrameSlot findFrameSlotInPrevious(int index) {
    if (this.previous == null) {
      new IllegalStateException("Previous FrameDescriptorPart is null");
    }
    return this.previous.findFrameSlot(index);
  }

  public FrameSlot findFrameSlot(int index) {
    return frame().findFrameSlot(startOffset + index);
  }

  public void setFrameSlotKind(FrameSlot slot, FrameSlotKind kind) {
    frame().setFrameSlotKind(slot, kind);
  }

  public void pushRelDataType(RelDataType relDataType) {
    this.relDataTypes.add(0, relDataType);
  }

  public RelDataType getRelDataType() {
    return this.relDataTypes.get(0);
  }
}