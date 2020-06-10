package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import net.wrap_trap.truffle_arrow.TruffleArrowConfig;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Objects;

/**
 * Compiles RexNode into ExprBase.
 * RexNode is Calcites representations of expressions like a+b, DATE_PART(...)
 * ExprBase is our representation of an executable expression:
 * an ExprBase reads column values from VirtualFrame and produces a value.
 */
public class CompileExpr implements RexVisitor<ExprBase> {

  public static ExprBase compile(FrameDescriptor from, RexNode child, SinkContext context) {
    CompileExpr compiler = new CompileExpr(from, context);

    return child.accept(compiler);
  }

  /**
   * FROM clause of SQL query.
   *
   * Can be empty in queries like SELECT 1
   */
  private final FrameDescriptor from;
  private SinkContext context;

  CompileExpr(FrameDescriptor from, SinkContext context) {
    this.from = from;
    this.context = context;
  }
  
  @Override
  public ExprBase visitInputRef(RexInputRef inputRef) {
    this.context.addInputRef(inputRef);
    //FrameSlotKind kind = getFrameSlotKind(inputRef.getType());
    FrameSlot slot = from.addFrameSlot(inputRef.getIndex());
    Objects.requireNonNull(slot);

    return ExprReadLocalNodeGen.create(slot);
  }

  @Override
  public ExprBase visitLocalRef(RexLocalRef localRef) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ExprBase visitLiteral(RexLiteral literal) {
    if (RexLiteral.isNullLiteral(literal))
      return ExprLiteral.Null();

    Object value = Types.coerceLiteral(literal);
    SqlTypeName type = literal.getType().getSqlTypeName();
    FrameSlotKind kind = Types.kind(type);

    switch (kind) {
      case Int:
        return ExprLiteral.Int((int) value);
      case Long:
        return ExprLiteral.Long((long) value);
      case Double:
        return ExprLiteral.Double((double) value);
      case Boolean:
        return ExprLiteral.Boolean((boolean) value);
      case Object:
        return ExprLiteral.Object(value);
      case Illegal:
      default:
        throw new RuntimeException("Don't know what to do with " + literal);
    }
  }

  @Override
  public ExprBase visitCall(RexCall call) {
    switch (call.getKind()) {
//      case TIMES:
//        return binary(call.getOperands(), ExprMultiplyNodeGen::create);
//      case DIVIDE:
//        return binary(call.getOperands(), ExprDivideNodeGen::create);
//      case PLUS:
//        return binary(call.getOperands(), ExprPlusNodeGen::create);
//      case MINUS:
//        return binary(call.getOperands(), ExprMinusNodeGen::create);
//      case IN:
//        throw new UnsupportedOperationException();
      case LESS_THAN:
        return binary(call.getOperands(), ExprLessThanNodeGen::create);
      case GREATER_THAN:
        if (containsInputRef(call.getOperands())) {
          return binary(call.getOperands(), ExprGreaterThanFilterNodeGen::create);
        }
        throw new UnsupportedOperationException();
      case LESS_THAN_OR_EQUAL:
        return binary(call.getOperands(), ExprLessThanOrEqualNodeGen::create);
      case GREATER_THAN_OR_EQUAL:
        if (containsInputRef(call.getOperands())) {
          return binary(call.getOperands(), ExprGreaterEqualFilterNodeGen::create);
        }
        throw new UnsupportedOperationException();
      case EQUALS:
        return binary(call.getOperands(), ExprEqualsNodeGen::create);
      case NOT_EQUALS:
        if (containsInputRef(call.getOperands())) {
          return binary(call.getOperands(), ExprNotEqualFilterNodeGen::create);
        }
        throw new UnsupportedOperationException();
      case IS_NULL:
        if (containsInputRef(call.getOperands())) {
          return ExprIsNullFilterNodeGen.create(compile(singleOperand(call.getOperands())));
        }
        throw new UnsupportedOperationException();
      case IS_NOT_NULL:
        return ExprIsNotNullFilterNodeGen.create(compile(singleOperand(call.getOperands())));
      case CAST:
        return ExprCastNodeGen.create(call.getType(), compile(singleOperand(call.getOperands())));
//      case OR:
//        return fold(call.getOperands(), 0, ExprOrNodeGen::create);
//      case AND:
//        return binary(call.getOperands(), ExprAndNodeGen::create);
//      case LIKE:
//        throw new UnsupportedOperationException();
//      case SIMILAR:
//        throw new UnsupportedOperationException();
//      case BETWEEN:
//        throw new UnsupportedOperationException();
//      case CASE:
//        return compileCase(call.getOperands(), 0);
//      case NULLIF:
//        throw new UnsupportedOperationException();
//      case COALESCE:
//        throw new UnsupportedOperationException();
//      case TIMESTAMP_ADD:
//        throw new UnsupportedOperationException();
//      case TIMESTAMP_DIFF:
//        throw new UnsupportedOperationException();
//      case NOT:
//        return ExprNotNodeGen.create(compile(singleOperand(call.getOperands())));
//      case PLUS_PREFIX:
//        throw new UnsupportedOperationException();
//      case MINUS_PREFIX:
//        throw new UnsupportedOperationException();
//      case ROW:
//        throw new UnsupportedOperationException();
//      case CAST:
//        return ExprCastNodeGen.create(call.getType(), compile(singleOperand(call.getOperands())));
//      case FLOOR:
//        throw new UnsupportedOperationException();
//      case CEIL:
//        throw new UnsupportedOperationException();
//      case TRIM:
//        throw new UnsupportedOperationException();
//      case LTRIM:
//        throw new UnsupportedOperationException();
//      case RTRIM:
//        throw new UnsupportedOperationException();
//      case EXTRACT:
//        throw new UnsupportedOperationException();
//      case UNNEST:
//        throw new UnsupportedOperationException();
//      case COUNT:
//        throw new UnsupportedOperationException();
//      case SUM:
//        throw new UnsupportedOperationException();
//      case MIN:
//        throw new UnsupportedOperationException();
//      case MAX:
//        throw new UnsupportedOperationException();
//      case LEAD:
//        throw new UnsupportedOperationException();
//      case LAG:
//        throw new UnsupportedOperationException();
//      case FIRST_VALUE:
//        throw new UnsupportedOperationException();
//      case LAST_VALUE:
//        throw new UnsupportedOperationException();
//      case COVAR_POP:
//        throw new UnsupportedOperationException();
//      case COVAR_SAMP:
//        throw new UnsupportedOperationException();
//      case AVG:
//        throw new UnsupportedOperationException();
//      case STDDEV_POP:
//        throw new UnsupportedOperationException();
//      case STDDEV_SAMP:
//        throw new UnsupportedOperationException();
//      case VAR_POP:
//        throw new UnsupportedOperationException();
//      case VAR_SAMP:
//        throw new UnsupportedOperationException();
//      case NTILE:
//        throw new UnsupportedOperationException();
//      case ROW_NUMBER:
//        throw new UnsupportedOperationException();
//      case RANK:
//        throw new UnsupportedOperationException();
//      case PERCENT_RANK:
//        throw new UnsupportedOperationException();
//      case DENSE_RANK:
//        throw new UnsupportedOperationException();
//      case CUME_DIST:
//        throw new UnsupportedOperationException();
      default:
        throw new RuntimeException("Don't know what to do with " + call.getKind());
    }
  }

  private RexNode singleOperand(List<RexNode> operands) {
    assert operands.size() == 1;

    return operands.get(0);
  }
//
//  private ExprBase compileCase(List<RexNode> operands, int offset) {
//    // ELSE ? END
//    if (offset == operands.size() - 1)
//      return compile(operands.get(offset));
//      // WHEN ? THEN ? ELSE ...
//    else {
//      ExprTest ifNode = ExprTestNodeGen.create(compile(operands.get(offset)));
//      ExprBase thenNode = compile(operands.get(offset + 1));
//      ExprBase elseNode = compileCase(operands, offset + 2);
//
//      return new ExprIf(ifNode, thenNode, elseNode);
//    }
//  }

  private ExprBase compile(RexNode rexNode) {
    return rexNode.accept(new CompileExpr(from, context));
  }

  @FunctionalInterface
  private interface BinaryConstructor {
    ExprBinary accept(ExprBase left, ExprBase right);
  }

  private ExprBase fold(List<RexNode> operands, int offset, BinaryConstructor reduce) {
    assert operands.size() > 0;

    ExprBase acc = compile(operands.get(operands.size() - 1));

    for (int i = operands.size() - 2; i >= 0; i--)
      acc = reduce.accept(compile(operands.get(i)), acc);

    return acc;
  }

  private ExprBase binary(List<RexNode> operands, BinaryConstructor then) {
    assert operands.size() == 2;

    ExprBase left = operands.get(0).accept(new CompileExpr(from, context));
    ExprBase right = operands.get(1).accept(new CompileExpr(from, context));

    return then.accept(left, right);
  }

  @Override
  public ExprBase visitOver(RexOver over) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ExprBase visitCorrelVariable(RexCorrelVariable correlVariable) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ExprBase visitDynamicParam(RexDynamicParam dynamicParam) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ExprBase visitRangeRef(RexRangeRef rangeRef) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ExprBase visitFieldAccess(RexFieldAccess fieldAccess) {
    ExprBase receiver = fieldAccess.getReferenceExpr().accept(new CompileExpr(from, context));
    String name = fieldAccess.getField().getName();

    return ExprReadPropertyNodeGen.create(name, receiver);
  }

  @Override
  public ExprBase visitSubQuery(RexSubQuery subQuery) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ExprBase visitTableInputRef(RexTableInputRef fieldRef) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ExprBase visitPatternFieldRef(RexPatternFieldRef fieldRef) {
    throw new UnsupportedOperationException();
  }

  private boolean containsInputRef(List<RexNode> rexNodes) {
    for (RexNode rexNode: rexNodes) {
      if (rexNode instanceof RexInputRef) {
        return true;
      }
    }
    return false;
  }
}
