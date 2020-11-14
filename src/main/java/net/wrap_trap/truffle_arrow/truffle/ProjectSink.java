package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import org.apache.calcite.rex.RexNode;

import java.util.List;


public class ProjectSink extends RelRowSink {

  public static ProjectSink createSink(
    FrameDescriptorPart framePart,
    List<? extends RexNode> projects,
    CompileContext compileContext,
    ThenRowSink next) {
    FrameDescriptorPart newFramePart = framePart.newPart();
    for (int i = 0; i < projects.size(); i ++) {
      newFramePart.addFrameSlot();
    }
    RowSink rowSink = next.apply(newFramePart);

    ExprBase[] locals = new ExprBase[projects.size()];
    for (int i = 0; i < projects.size(); i ++) {
      RexNode child = projects.get(i);
      locals[i] = compile(newFramePart, child, compileContext);
    }
    return new ProjectSink(newFramePart, locals, rowSink);
  }

  private static ExprBase compile(FrameDescriptorPart framePart, RexNode child, CompileContext compileContext) {
    return ProjectCompileExpr.compile(framePart, child, compileContext);
  }

  private FrameDescriptorPart projectFramePart;
  private VirtualFrame projectFrame;
  private ExprBase[] locals;

  private ProjectSink(FrameDescriptorPart projectFramePart, ExprBase[] locals, RowSink then) {
    super(then);
    this.projectFramePart = projectFramePart;
    this.locals = locals;
    this.projectFrame = Truffle.getRuntime()
                              .createVirtualFrame(new Object[] { }, this.projectFramePart.frame());

  }

  @Override
  public void executeByRow(VirtualFrame frame, FrameDescriptorPart framePart, SinkContext context) throws UnexpectedResultException {
    for (int i = 0; i < this.locals.length; i ++) {
      Object v = locals[i].executeGeneric(frame);
      StatementWriteLocalNodeGen.create(
          ExprLiteral.Object(v), projectFramePart.findFrameSlot(i)).executeVoid(this.projectFrame);
    }
    then.executeByRow(this.projectFrame, this.projectFramePart, context);
  }
}