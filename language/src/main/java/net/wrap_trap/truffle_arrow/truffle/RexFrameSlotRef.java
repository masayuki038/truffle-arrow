package net.wrap_trap.truffle_arrow.truffle;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBiVisitor;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitor;

import java.util.Objects;

public class RexFrameSlotRef extends RexNode {

  private int index;

  public RexFrameSlotRef(int index) {
    this.index = index;
  }

  public int getIndex() {
    return this.index;
  }

  @Override
  public RelDataType getType() {
    throw new UnsupportedOperationException();
  }

  @Override
  public <R> R accept(RexVisitor<R> visitor) {
    if (visitor instanceof TruffleArrowRexVisitor) {
      return ((TruffleArrowRexVisitor<R>) visitor).visitFrameSlotRef(this);
    }
    throw new UnsupportedOperationException();
  }

  @Override
  public <R, P> R accept(RexBiVisitor<R, P> visitor, P arg) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof RexFrameSlotRef) {
      return Objects.equals(this.index, ((RexFrameSlotRef) obj).index);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(index);
  }
}
