package net.wrap_trap.truffle_arrow.truffle;

import java.util.Objects;

public class InputRefSlotMap {

  private int index;
  private int slot;

  public InputRefSlotMap(int index, int slot) {
    this.index = index;
    this.slot = slot;
  }

  public int getIndex() {
    return index;
  }

  public int getSlot() {
    return slot;
  }

  @Override
  public int hashCode() {
    return Objects.hash(index, slot);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    InputRefSlotMap that = (InputRefSlotMap) o;
    return index == that.index &&
             slot == that.slot;
  }
}
