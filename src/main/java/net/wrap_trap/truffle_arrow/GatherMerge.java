package net.wrap_trap.truffle_arrow;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import net.wrap_trap.truffle_arrow.truffle.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;

import java.util.List;

public class GatherMerge extends SingleRel implements ArrowRel {

  public GatherMerge(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
    super(cluster, traits, input);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert(inputs.size() == 1);
    return new GatherMerge(getCluster(), traitSet, inputs.get(0));
  }

  @Override
  public ThenRowSink createRowSink(ThenRowSink next, CompileContext context) {
    return
      sourceFrame -> createSink(sourceFrame, context, next);
  }

  public RelRowSink createSink(FrameDescriptorPart framePart, CompileContext context, ThenRowSink next) {
    RowSink rowSink = next.apply(framePart);
    return new RelRowSink(rowSink) {
      @Override
      public void executeByRow(VirtualFrame frame, FrameDescriptorPart framePart, SinkContext context) throws UnexpectedResultException {
        then.executeByRow(frame, framePart, context);
      }
    };
  }
}
