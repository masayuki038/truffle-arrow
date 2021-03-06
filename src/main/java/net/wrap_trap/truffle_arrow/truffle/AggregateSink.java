package net.wrap_trap.truffle_arrow.truffle;

import com.google.common.collect.ImmutableList;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import net.wrap_trap.truffle_arrow.ArrowUtils;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class AggregateSink extends RelRowSink {
  private static final Logger log = LoggerFactory.getLogger(AggregateSink.class);

  public static AggregateSink createSink(
    FrameDescriptorPart framePart,
    ImmutableBitSet groupSet,
    ImmutableList<ImmutableBitSet> groupSets,
    List<AggregateCall> aggCalls,
    RelDataType relDataType,
    CompileContext context,
    ThenRowSink next) {
    log.debug("createSink");

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
            aggregateFramePart.findFrameSlotInPrevious(aggCall.getArgList().get(0)),
            child -> this.insert(child))
          );
          break;
        default:
          throw new UnsupportedOperationException("Unsupported function: " + kind);
      }
    }
  }

  @Override
  protected FrameDescriptorPart getFrameDescriptorPart() {
    return this.getFrameDescriptorPart();
  }

  @Override
  public SinkContext executeByRow(VirtualFrame frame, FrameDescriptorPart framePart, SinkContext context) {

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
    List<Object> funcResults = this.aggFunctions.stream().map(f -> {
      try {
        return f.executeGeneric(frame);
      } catch (Throwable e) {
        log.error("executeByRow failed", e);
        throw e;
      }
    }).collect(Collectors.toList());
    map.put(grouping, funcResults);
    return context;
  }

  @Override
  public SinkContext afterExecute(VirtualFrame frame, SinkContext initialContext) throws UnexpectedResultException {
    log.debug("afterExecute");

    SinkContext context = initialContext;
    for (List<Object> keyList : this.map.keySet()) {
      int i;
      for (i = 0; i < keyList.size(); i ++) {
        StatementWriteLocalNodeGen.create(
          ExprLiteral.Object(keyList.get(i)), this.aggregateFramePart.findFrameSlot(i)).executeVoid(frame);
      }
      List<Object> funcResults = this.map.get(keyList);
      for (int j = 0; j < funcResults.size(); j ++) {
        ExprBase result = ExprCastNodeGen.create(
          aggCalls.get(j).getType(),
          ExprLiteral.Object(funcResults.get(j)));
        StatementWriteLocalNodeGen.create(
          result,
          this.aggregateFramePart.findFrameSlot(i ++)
        ).executeVoid(frame);
      }
      context = then.executeByRow(frame, this.aggregateFramePart, context);
    }
    return then.afterExecute(frame, context);
  }
}
