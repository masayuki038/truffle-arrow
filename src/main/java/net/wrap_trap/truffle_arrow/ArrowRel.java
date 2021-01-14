package net.wrap_trap.truffle_arrow;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import net.wrap_trap.truffle_arrow.truffle.*;
import net.wrap_trap.truffle_arrow.truffle.node.GroupLeaderNode;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

import java.util.Vector;

/**
 * Relational expression that uses Arrow calling convention.
 */
public interface ArrowRel extends RelNode {
  Convention CONVENTION = new Convention.Impl("ARROW", ArrowRel.class);

  RelNode getInput();

  ThenRowSink createRowSink(ThenRowSink next, CompileContext context);

  default ThenLeader createLeader(ThenRowSink next, CompileContext context) {
    throw new UnsupportedOperationException();
  }

  default boolean isLeader() {
    return false;
  }

  default RowSource compile(CompileContext context) {
    ThenRowSink createConverter = sourceFrame -> VectorSchemaRootConverterSink.createSink(sourceFrame, context, null);
    return compile(createConverter, context);
  }

  default RowSource compile(ThenRowSink next, CompileContext context) {
    ThenRowSink nextWorkerSink;
    if (isLeader()) {
      ThenLeader leader = createLeader(next, context);
      context.addLeader(leader);
      nextWorkerSink = sourceFrame -> VectorSchemaRootConverterSink.createSink(sourceFrame, context, null);
    } else {
      nextWorkerSink = createRowSink(next, context);
    }

    final ThenRowSink l1s = nextWorkerSink;
    RelNode input = getInput();
    if (input != null) {
      ArrowRel arrowRel = (ArrowRel) input;
      if (arrowRel.isLeader()) {
        // L1 を作る
        ThenLeader leader = GroupLeaderNode.getFactory(nextWorkerSink, context);
        context.addLeader(leader);
        nextWorkerSink = sourceFrame -> VectorSchemaRootConverterSink.createSink(sourceFrame, context, null);
      }
      return arrowRel.compile(nextWorkerSink, context);
    }

    // L2 作る
    ThenLeader leader = GroupLeaderNode.getFactory(nextWorkerSink, context);
    context.addLeader(leader);

    return TerminalSink.compile(context);
  }
}
