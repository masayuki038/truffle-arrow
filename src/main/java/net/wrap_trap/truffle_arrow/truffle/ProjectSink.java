package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotTypeException;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import org.apache.arrow.vector.FieldVector;

import java.util.ArrayList;
import java.util.List;


public class ProjectSink extends RowSink {

  public static ProjectSink createSink(FrameDescriptor frameDescriptor, int[] projectIndex, ThenRowSink next) {
    RowSink rowSink = next.apply(frameDescriptor);
    return new ProjectSink(projectIndex, rowSink);
  }

  private int[] projectIndex;
  private RowSink then;

  private ProjectSink(int[] projectIndex, RowSink then) {
    this.projectIndex = projectIndex;
    this.then = then;
  }

  @Override
  public void executeVoid(VirtualFrame frame, FrameDescriptor frameDescriptor, SinkContext context) throws UnexpectedResultException {
    List<FieldVector> input = context.vectors();
    List<FieldVector> projected = new ArrayList<FieldVector>();
    for (int i = 0; i < this.projectIndex.length; i ++) {
      projected.add(input.get(projectIndex[i]));
    }
    context.setVectors(projected);
    then.executeVoid(frame, frameDescriptor, context);
  }
}