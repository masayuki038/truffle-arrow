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
    ThenRowSink createConverter = sourceFrame -> createSink(sourceFrame, context, null);
    return compile(createConverter, context);
  }

  default RowSource compile(ThenRowSink next, CompileContext context) {
    ThenRowSink nextWorkerSink;
    if (isLeader()) {
      ThenLeader leader = createLeader(next, context);
      context.addLeader(leader);
      nextWorkerSink = sourceFrame -> createSink(sourceFrame, context, null);
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
        nextWorkerSink = sourceFrame -> createSink(sourceFrame, context, null);
      }
      return arrowRel.compile(nextWorkerSink, context);
    }

    // L2 作る
    ThenLeader leader = GroupLeaderNode.getFactory(nextWorkerSink, context);
    context.addLeader(leader);

    return TerminalSink.compile(context);
  }

  // TODO Move to ConverterSink
  default RelRowSink createSink(FrameDescriptorPart framePart, CompileContext context, ThenRowSink next) {
    // TODO とりあえず 1 つの VectorSchemaRoot に入れる。あとで複数に分割することを検討する
    VectorSchemaRoot vectorSchemaRoot = ArrowUtils.createVectorSchemaRoot(framePart);
    return new RelRowSink(null) {
      int index = 0;

      @Override
      protected FrameDescriptorPart getFrameDescriptorPart() {
        throw new UnsupportedOperationException();
      }

      @Override
      public SinkContext executeByRow(VirtualFrame frame, FrameDescriptorPart framePart, SinkContext context) throws UnexpectedResultException {
        ArrowUtils.setValues(frame, framePart, vectorSchemaRoot, this.index ++);
        return context;
      }

      @Override
      public SinkContext afterExecute(VirtualFrame frame, SinkContext context) throws UnexpectedResultException {
        vectorSchemaRoot.getFieldVectors().stream().forEach(fieldVector -> fieldVector.setValueCount(this.index));
        vectorSchemaRoot.setRowCount(this.index);
        return context.setVectorSchemaRoots(new VectorSchemaRoot[]{vectorSchemaRoot});
      }
    };
  }
}
