package net.wrap_trap.truffle_arrow;

import org.apache.calcite.plan.Context;

public class PlannerContext implements Context {
  @Override
  public <C> C unwrap(Class<C> aClass) {
    return null;
  }
}
