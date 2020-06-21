package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotTypeException;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import org.apache.arrow.vector.FieldVector;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;


public class ProjectSink extends RowSink {

  public static ProjectSink createSink(
    FrameDescriptor frameDescriptor, List<? extends RexNode> projects, SinkContext context, ThenRowSink next) {
    RowSink rowSink = next.apply(frameDescriptor);

    StatementWriteLocal[] locals = new StatementWriteLocal[projects.size()];
    for (int i = 0; i < projects.size(); i ++) {
      RexNode child = projects.get(i);
      ExprBase compiled = compile(frameDescriptor, child, context);
      FrameSlot slot = frameDescriptor.findFrameSlot(i);
      locals[i] = StatementWriteLocalNodeGen.create(compiled, slot);
    }
    return new ProjectSink(locals, rowSink);
  }

  private static ExprBase compile(FrameDescriptor frameDescriptor, RexNode child, SinkContext context) {
    return child.accept(new CompileExpr(frameDescriptor, context));
  }

  private StatementWriteLocal[] locals;
  private RowSink then;

  private ProjectSink(StatementWriteLocal[] locals, RowSink then) {
    this.locals = locals;
    this.then = then;
  }

  @Override
  public void executeVoid(VirtualFrame frame, FrameDescriptor frameDescriptor, SinkContext context) throws UnexpectedResultException {
    this.vectorEach(frame, frameDescriptor, context, i -> {
      try {
        this.executeByRow(frame, frameDescriptor, context);
        this.then.executeByRow(frame, frameDescriptor, context);
      } catch (UnexpectedResultException e) {
        throw new RuntimeException(e);
      }
    });

    then.executeVoid(frame, frameDescriptor, context);
  }

  @Override
  public void executeByRow(VirtualFrame frame, FrameDescriptor frameDescriptor, SinkContext context) throws UnexpectedResultException {
    for (StatementWriteLocal local : locals) {
      local.executeVoid(frame);
    }
  }
}