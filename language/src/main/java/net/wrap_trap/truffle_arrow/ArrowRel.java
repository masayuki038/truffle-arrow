package net.wrap_trap.truffle_arrow;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import net.wrap_trap.truffle_arrow.truffle.*;
import net.wrap_trap.truffle_arrow.truffle.node.FirstGroupLeaderNode;
import net.wrap_trap.truffle_arrow.truffle.node.GroupLeaderNode;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

import java.util.Vector;

/**
 * Relational expression that uses Arrow calling convention.
 */
public interface ArrowRel extends RelNode {
  Convention CONVENTION = new Convention.Impl("ARROW", ArrowRel.class);

  RelNode getInput();

  RelDataType getRelDataType();

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

    RelNode input = getInput();
    if (input != null) {
      ArrowRel arrowRel = (ArrowRel) input;
      if (arrowRel.isLeader()) {
        // L2 を作る
        final ThenRowSink secondWorkerSink = nextWorkerSink;
        RelDataType relDataType = arrowRel.getRelDataType();
        ThenRowSink groupBootstrapSink = sourceFrame -> GroupBootstrapSink.createSink(sourceFrame, relDataType, context, secondWorkerSink);
        ThenLeader leader = GroupLeaderNode.getFactory(groupBootstrapSink, relDataType, context);
        context.addLeader(leader);
        nextWorkerSink = sourceFrame -> VectorSchemaRootConverterSink.createSink(sourceFrame, context, null);
      }
      return arrowRel.compile(nextWorkerSink, context);
    }

    // L1 作る
    ThenLeader leader = FirstGroupLeaderNode.getFactory(nextWorkerSink, context);
    context.setFirstLeader(leader);

    return TerminalSink.compile(context);
  }
}
