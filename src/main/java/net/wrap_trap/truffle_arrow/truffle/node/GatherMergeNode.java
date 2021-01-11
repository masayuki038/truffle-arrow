package net.wrap_trap.truffle_arrow.truffle.node;

import com.google.common.collect.ImmutableList;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import net.wrap_trap.truffle_arrow.ArrowUtils;
import net.wrap_trap.truffle_arrow.truffle.*;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.*;

public class GatherMergeNode  extends AbstractLeaderNode {

  public static GatherMergeNode createSink(
    FrameDescriptorPart framePart,
    ImmutableBitSet groupSet,
    ImmutableList<ImmutableBitSet> groupSets,
    List<AggregateCall> aggCalls,
    RelDataType relDataType,
    CompileContext context,
    ThenRowSink next) {
    FrameDescriptorPart aggregateFramePart = framePart.newPart();
    aggregateFramePart.pushRelDataType(relDataType);
    for (Integer i : groupSet) {
      FrameSlot slot = aggregateFramePart.addFrameSlot();
      FrameSlotKind slotKind = ArrowUtils.getFrameSlotKind(relDataType.getFieldList().get(i));
      aggregateFramePart.setFrameSlotKind(slot, slotKind);
    }
    for (AggregateCall aggCall: aggCalls) {
      aggregateFramePart.addFrameSlot();
      FrameSlot frameSlot = aggregateFramePart.findFrameSlot(aggregateFramePart.getCurrentSlotPosition());
      frameSlot.setKind(FrameSlotKind.Long);
    }

    RowSink rowSink = GatherMergeSink.createRowSink(aggregateFramePart, groupSet, groupSets, aggCalls, relDataType, context, next);
    return new GatherMergeNode(aggregateFramePart, groupSet, groupSets, aggCalls, context, rowSink);
  }

  private FrameDescriptorPart aggregateFramePart;
  private RowSink rowSink;
  private SinkContext sinkContext;

  private GatherMergeNode(
    FrameDescriptorPart aggregateFramePart,
    ImmutableBitSet groupSet,
    ImmutableList<ImmutableBitSet> groupSets,
    List<AggregateCall> aggCalls,
    CompileContext compileContext,
    RowSink rowSink) {
    this.aggregateFramePart = aggregateFramePart;
    this.rowSink = rowSink;
    Set<InputRefSlotMap> slotMap = new HashSet<>();
    for (int i = 0; i < aggregateFramePart.getFrameSlots().size(); i ++) {
      slotMap.add(new InputRefSlotMap(i, i));
    }
    this.sinkContext = new SinkContext(slotMap, null, new ArrayList<Row>());
    this.insert(rowSink);
  }

  @Override
  public VectorSchemaRoot[] execute(VectorSchemaRoot[] vectorSchemaRoots) {
    VirtualFrame frame = Truffle.getRuntime()
                           .createVirtualFrame(new Object[] { }, this.aggregateFramePart.frame());
    try {
      this.sinkContext = rowSink.executeVoid(frame, vectorSchemaRoots, this.sinkContext);
      this.sinkContext = rowSink.afterExecute(frame, this.sinkContext);
      return this.sinkContext.getVectorSchemaRoots();
    } catch (UnexpectedResultException e) {
      throw new RuntimeException(e);
    }
  }
}
