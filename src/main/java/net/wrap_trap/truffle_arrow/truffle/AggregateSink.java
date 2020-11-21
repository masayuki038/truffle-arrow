package net.wrap_trap.truffle_arrow.truffle;

import com.google.common.collect.ImmutableList;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.*;
import java.util.stream.Collectors;

public class AggregateSink extends RelRowSink {

  public static AggregateSink createSink(
    FrameDescriptorPart framePart,
    ImmutableBitSet groupSet,
    ImmutableList<ImmutableBitSet> groupSets,
    List<AggregateCall> aggCalls,
    CompileContext context,
    ThenRowSink next) {
    FrameDescriptorPart aggregateFramePart = framePart.newPart();
    for (int i = 0; i < groupSet.toList().size(); i ++) {
      aggregateFramePart.addFrameSlot();
    }

    FrameDescriptorPart resultFramePart = aggregateFramePart.newPart();
    for (int i = 0; i < groupSet.toList().size(); i ++) {
      resultFramePart.addFrameSlot();
    }
    for (AggregateCall aggCall: aggCalls) {
      resultFramePart.addFrameSlot();
    }

    RowSink rowSink = next.apply(resultFramePart);

    return new AggregateSink(
        aggregateFramePart, resultFramePart, groupSet, groupSets, aggCalls, context, rowSink);
  }

  private FrameDescriptorPart aggregateFramePart;
  private FrameDescriptorPart resultFramePart;
  private ImmutableBitSet groupSet;
  private ImmutableList<ImmutableBitSet> groupSets;
  private List<AggregateCall> aggCalls;
  private CompileContext compileContext;
  private Map<List<Object>, List<Object>> map;
  private List<TANewObject> objects;
  private FrameSlot keyFrameSlot;
  private List<FrameSlot> receiverFrameSlots;
  private List<ExprBase> aggFunctions;
  private VirtualFrame aggregateFrame;

  private AggregateSink(
    FrameDescriptorPart aggregateFramePart,
    FrameDescriptorPart resultFramePart,
    ImmutableBitSet groupSet,
    ImmutableList<ImmutableBitSet> groupSets,
    List<AggregateCall> aggCalls,
    CompileContext compileContext,
    RowSink then) {
    super(then);
    this.aggregateFramePart = aggregateFramePart;
    this.resultFramePart = resultFramePart;
    this.groupSet = groupSet;
    this.groupSets = groupSets;
    this.aggCalls = aggCalls;
    this.compileContext = compileContext;
    this.map = new HashMap<>();
    this.aggFunctions = new ArrayList<>();
    this.keyFrameSlot = this.aggregateFramePart.frame().addFrameSlot("tmpKey", FrameSlotKind.Object);
    this.receiverFrameSlots = new ArrayList<>();
    this.objects = new ArrayList<>();
    this.aggregateFrame = Truffle.getRuntime()
                              .createVirtualFrame(new Object[] { }, this.aggregateFramePart.frame());

    for (AggregateCall aggCall: this.aggCalls) {
      SqlKind kind = aggCall.getAggregation().kind;
      switch(kind) {
        case COUNT:
          aggFunctions.add(Functions.count(
            this.aggregateFramePart,
            this.receiverFrameSlots,
            this.keyFrameSlot,
            child -> this.insert(child))
          );
          break;

        default:
          throw new UnsupportedOperationException("Unsupported function: " + kind);
      }
    }
  }

  @Override
  public void executeByRow(VirtualFrame frame, FrameDescriptorPart framePart, SinkContext context) {

    if (this.receiverFrameSlots.size() > 0 && this.objects.size() == 0) {
      for (FrameSlot receiverFrameSlot: this.receiverFrameSlots) {
        TANewObject object = TANewObjectNodeGen.create();
        this.objects.add(object);
        StatementWriteLocalNodeGen.create(object, receiverFrameSlot).executeVoid(this.aggregateFrame);
      }
    }
    List<Object> grouping = new ArrayList<>();
    for (Integer i : groupSet) {
      FrameSlot slot = this.aggregateFramePart.findFrameSlotInPrevious(i);
      Objects.requireNonNull(slot);
      Object value = frame.getValue(slot);
      grouping.add(value);
    }
    StatementWriteLocalNodeGen.create(
      ExprLiteral.Object(grouping.toString()), this.keyFrameSlot).executeVoid(this.aggregateFrame);
    List<Object> funcResults = this.aggFunctions.stream().map(f -> f.executeGeneric(this.aggregateFrame))
                                 .collect(Collectors.toList());
    map.put(grouping, funcResults);
  }

  @Override
  public void afterExecute(VirtualFrame frame, SinkContext context) throws UnexpectedResultException {
    VirtualFrame resultFrame = Truffle.getRuntime()
                                .createVirtualFrame(new Object[] { }, this.resultFramePart.frame());

    for (List<Object> keyList : this.map.keySet()) {
      int i;
      for (i = 0; i < keyList.size(); i ++) {
        StatementWriteLocalNodeGen.create(
          ExprLiteral.Object(keyList.get(i)), this.resultFramePart.findFrameSlot(i)).executeVoid(resultFrame);
      }
      for (Object funcResult: this.map.get(keyList)) {
        StatementWriteLocalNodeGen.create(
          ExprLiteral.Object(funcResult), this.resultFramePart.findFrameSlot(i ++)).executeVoid(resultFrame);
      }
      then.executeByRow(resultFrame, this.resultFramePart, context);
    }
    then.afterExecute(resultFrame, context);
  }
}
