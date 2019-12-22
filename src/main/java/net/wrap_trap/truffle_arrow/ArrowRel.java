package net.wrap_trap.truffle_arrow;

import com.google.common.collect.Maps;
import net.wrap_trap.truffle_arrow.truffle.RowSource;
import net.wrap_trap.truffle_arrow.truffle.ThenRowSink;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

import java.util.Map;

/**
 * Relational expression that uses Arrow calling convention.
 */
public interface ArrowRel extends RelNode {
  Convention CONVENTION = new Convention.Impl("ARROW", ArrowRel.class);

  RowSource compile(ThenRowSink next);
}
