package net.wrap_trap.truffle_arrow;

import net.wrap_trap.truffle_arrow.truffle.*;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

/**
 * Relational expression that uses Arrow calling convention.
 */
public interface ArrowRel extends RelNode {
  Convention CONVENTION = new Convention.Impl("ARROW", ArrowRel.class);

  RelNode getInput();

  ThenRowSink createRowSink(ThenRowSink next, CompileContext context);

  default RowSource compile(ThenRowSink next, CompileContext context) {
    ThenRowSink wrapped = createRowSink(next, context);

    RelNode input = getInput();
    if (input != null) {
      ArrowRel arrowRel = (ArrowRel) input;
      return arrowRel.compile(wrapped, context);
    }
    return TerminalSink.compile(context, wrapped);
  }
}
