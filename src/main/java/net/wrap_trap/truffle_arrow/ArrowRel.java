package net.wrap_trap.truffle_arrow;

import net.wrap_trap.truffle_arrow.truffle.RowSource;
import net.wrap_trap.truffle_arrow.truffle.ThenRowSink;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

/**
 * Relational expression that uses Arrow calling convention.
 */
public interface ArrowRel extends RelNode {
  Convention CONVENTION = new Convention.Impl("ARROW", ArrowRel.class);

  RowSource compile(ThenRowSink next);
}
