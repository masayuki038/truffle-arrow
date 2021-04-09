package net.wrap_trap.truffle_arrow;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;

public class LastPlan {

  static LastPlan INSTANCE = new LastPlan();

  private RelRoot lastPlan;

  private LastPlan() { }

  public void set(RelRoot relRoot) {
    this.lastPlan = relRoot;
  }

  public boolean includes(Class<? extends ArrowRel> clazz) {
    assert this.lastPlan != null;

    RelNode relNode = this.lastPlan.rel;
    do {
      assert relNode instanceof ArrowRel;
      if (relNode.getClass().isAssignableFrom(clazz)) {
        return true;
      }
      relNode = ((ArrowRel) relNode).getInput();
    } while (relNode != null);

    return false;
  }
}
