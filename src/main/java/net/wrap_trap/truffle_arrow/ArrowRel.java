package net.wrap_trap.truffle_arrow;

import net.wrap_trap.truffle_arrow.truffle.FilterSink;
import net.wrap_trap.truffle_arrow.truffle.RowSource;
import net.wrap_trap.truffle_arrow.truffle.TerminalSink;
import net.wrap_trap.truffle_arrow.truffle.ThenRowSink;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

/**
 * Relational expression that uses Arrow calling convention.
 */
public interface ArrowRel extends RelNode {
  Convention CONVENTION = new Convention.Impl("ARROW", ArrowRel.class);

  RelNode getInput();

  ThenRowSink createRowSink(ThenRowSink next);

  default RowSource compile(ThenRowSink next) {
    ThenRowSink wrapped = createRowSink(next);

    RelNode input = getInput();
    if (input != null) {
      ArrowRel arrowRel = (ArrowRel) input;
      return arrowRel.compile(wrapped);
    }
    return TerminalSink.compile(wrapped);
  }
}
