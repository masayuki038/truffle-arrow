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
    for (AggregateCall aggCall: aggCalls) {
      aggregateFramePart.addFrameSlot();
    }

    RowSink rowSink = next.apply(aggregateFramePart);

    return new AggregateSink(aggregateFramePart, groupSet, groupSets, aggCalls, context, rowSink);
  }

  private FrameDescriptorPart aggregateFramePart;
  private ImmutableBitSet groupSet;
  private ImmutableList<ImmutableBitSet> groupSets;
  private List<AggregateCall> aggCalls;
  private CompileContext compileContext;
  private Map<List<Object>, List<Object>> map;
  private List<TANewObject> objects;
  private FrameSlot keyFrameSlot;
  private List<FrameSlot> receiverFrameSlots;
  private List<ExprBase> aggFunctions;

  private AggregateSink(
    FrameDescriptorPart aggregateFramePart,
    ImmutableBitSet groupSet,
    ImmutableList<ImmutableBitSet> groupSets,
    List<AggregateCall> aggCalls,
    CompileContext compileContext,
    RowSink then) {
    super(then);
    this.aggregateFramePart = aggregateFramePart;
    this.groupSet = groupSet;
    this.groupSets = groupSets;
    this.aggCalls = aggCalls;
    this.compileContext = compileContext;
    this.map = new HashMap<>();
    this.aggFunctions = new ArrayList<>();
    this.keyFrameSlot = this.aggregateFramePart.frame().addFrameSlot("tmpKey", FrameSlotKind.Object);
    this.receiverFrameSlots = new ArrayList<>();
    this.objects = new ArrayList<>();

    for (AggregateCall aggCall: this.aggCalls) {
      SqlKind kind = aggCall.getAggregation().kind;
      FrameSlot receiverFrameSlot = aggregateFramePart.frame().addFrameSlot(
        "receiver" + receiverFrameSlots.size(), FrameSlotKind.Object);
      receiverFrameSlots.add(receiverFrameSlot);
      switch(kind) {
        case COUNT:
          aggFunctions.add(Functions.count(
            receiverFrameSlot,
            this.keyFrameSlot,
            child -> this.insert(child))
          );
          break;
        case SUM:
          aggFunctions.add(Functions.sum(
            receiverFrameSlot,
            this.keyFrameSlot,
            aggregateFramePart.findFrameSlot(aggCall.getArgList().get(0)),
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

    // TODO move to init
    if (this.receiverFrameSlots.size() > 0 && this.objects.size() == 0) {
      for (FrameSlot receiverFrameSlot: this.receiverFrameSlots) {
        TANewObject object = TANewObjectNodeGen.create();
        this.objects.add(object); // TODO remove this.objects after moving this statement to init
        StatementWriteLocalNodeGen.create(object, receiverFrameSlot).executeVoid(frame);
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
      ExprLiteral.Object(grouping.toString()), this.keyFrameSlot).executeVoid(frame);
    List<Object> funcResults = this.aggFunctions.stream().map(f -> f.executeGeneric(frame))
                                 .collect(Collectors.toList());
    map.put(grouping, funcResults);
  }

  @Override
  public void afterExecute(VirtualFrame frame, SinkContext context) throws UnexpectedResultException {
    for (List<Object> keyList : this.map.keySet()) {
      int i;
      for (i = 0; i < keyList.size(); i ++) {
        StatementWriteLocalNodeGen.create(
          ExprLiteral.Object(keyList.get(i)), this.aggregateFramePart.findFrameSlot(i)).executeVoid(frame);
      }
      for (Object funcResult: this.map.get(keyList)) {
        StatementWriteLocalNodeGen.create(
          ExprLiteral.Object(funcResult), this.aggregateFramePart.findFrameSlot(i ++)).executeVoid(frame);
      }
      then.executeByRow(frame, this.aggregateFramePart, context);
    }
    then.afterExecute(frame, context);
  }
}
