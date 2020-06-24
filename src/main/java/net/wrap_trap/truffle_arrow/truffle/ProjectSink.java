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
    FrameDescriptorPart framePart,
    List<? extends RexNode> projects,
    SinkContext context,
    ThenRowSink next) {
    FrameDescriptorPart newFramePart = framePart.newPart();
    for (int i = 0; i < projects.size(); i ++) {
      newFramePart.addFrameSlot(i);
    }
    RowSink rowSink = next.apply(newFramePart);

    StatementWriteLocal[] locals = new StatementWriteLocal[projects.size()];
    for (int i = 0; i < projects.size(); i ++) {
      RexNode child = projects.get(i);
      ExprBase compiled = compile(newFramePart, child, context);
      FrameSlot slot = newFramePart.findFrameSlot(i);
      locals[i] = StatementWriteLocalNodeGen.create(compiled, slot);
    }
    return new ProjectSink(newFramePart, locals, rowSink);
  }

  private static ExprBase compile(FrameDescriptorPart framePart, RexNode child, SinkContext context) {
    return child.accept(new CompileExpr(framePart, context));
  }

  private FrameDescriptorPart framePart;
  private StatementWriteLocal[] locals;
  private RowSink then;

  private ProjectSink(FrameDescriptorPart framePart, StatementWriteLocal[] locals, RowSink then) {
    this.framePart = framePart;
    this.locals = locals;
    this.then = then;
  }

  @Override
  public void executeVoid(VirtualFrame frame, SinkContext context) throws UnexpectedResultException {
    this.vectorEach(frame, this.framePart, context, i -> {
      try {
        this.executeByRow(frame, this.framePart, context);
        this.then.executeByRow(frame, this.framePart, context);
      } catch (UnexpectedResultException e) {
        throw new RuntimeException(e);
      }
    });

    then.executeVoid(frame, context);
  }

  @Override
  public void executeByRow(VirtualFrame frame, FrameDescriptorPart framePart, SinkContext context) throws UnexpectedResultException {
    for (StatementWriteLocal local : locals) {
      local.executeVoid(frame);
    }
    then.executeByRow(frame, framePart, context);
  }
}