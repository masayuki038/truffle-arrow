package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.Objects;

/**
 * Compiles RexNode into ExprBase.
 * RexNode is Calcites representations of expressions like a+b, DATE_PART(...)
 * ExprBase is our representation of an executable expression:
 * an ExprBase reads column values from VirtualFrame and produces a value.
 */
public abstract class CompileExpr implements TruffleArrowRexVisitor<ExprBase> {

  /**
   * FROM clause of SQL query.
   *
   * Can be empty in queries like SELECT 1
   */
  protected final FrameDescriptorPart from;
  protected CompileContext compileContext;

  CompileExpr(FrameDescriptorPart from, CompileContext compileContext) {
    this.from = from;
    this.compileContext = compileContext;
  }
  
  abstract public ExprBase visitInputRef(RexInputRef inputRef);

  abstract protected CompileExpr createCompileExpr(FrameDescriptorPart from, CompileContext context);

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
      case PLUS:
        return binary(call.getOperands(), ExprPlusNodeGen::create);
//      case MINUS:
//        return binary(call.getOperands(), ExprMinusNodeGen::create);
//      case IN:
//        throw new UnsupportedOperationException();
      case LESS_THAN:
        return binary(call.getOperands(), ExprLessThanNodeGen::create);
      case GREATER_THAN:
        return binary(call.getOperands(), ExprGreaterThanNodeGen::create);
      case LESS_THAN_OR_EQUAL:
        return binary(call.getOperands(), ExprLessThanOrEqualNodeGen::create);
      case GREATER_THAN_OR_EQUAL:
        return binary(call.getOperands(), ExprGreaterThanOrEqualNodeGen::create);
      case EQUALS:
        return binary(call.getOperands(), ExprEqualsNodeGen::create);
      case NOT_EQUALS:
        return ExprNotNodeGen.create(binary(call.getOperands(), ExprEqualsNodeGen::create));
      case IS_NULL:
        return isNull(call.getOperands());
      case IS_NOT_NULL:
        return ExprNotNodeGen.create(isNull(call.getOperands()));
      case CAST:
        return ExprCastNodeGen.create(call.getType(), compile(singleOperand(call.getOperands())));
      case OR:
        return fold(call.getOperands(), 0, ExprOrNodeGen::create);
      case AND:
        return fold(call.getOperands(), 0, ExprAndNodeGen::create);
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
      case COUNT:
        return count(call.getOperands());
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
    return rexNode.accept(createCompileExpr(from, this.compileContext));
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

  private ExprBase isNull(List<RexNode> operands) {
    assert operands.size() == 1;

    ExprBase left = operands.get(0).accept(createCompileExpr(from, this.compileContext));
    ExprBase right = ExprLiteral.Null();

    return ExprEqualsNodeGen.create(left, right);
  }

  private ExprBase binary(List<RexNode> operands, BinaryConstructor then) {
    assert operands.size() == 2;

    ExprBase left = operands.get(0).accept(createCompileExpr(from, this.compileContext));
    ExprBase right = operands.get(1).accept(createCompileExpr(from, this.compileContext));

    return then.accept(left, right);
  }

  private ExprBase count(List<RexNode> operands) {
    assert operands.size() == 1;

    ExprBase accumulator = operands.get(0).accept(createCompileExpr(from, this.compileContext));
    return ExprPlusNodeGen.create(accumulator, ExprLiteral.Int(1));
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
    ExprBase receiver = fieldAccess.getReferenceExpr().accept(createCompileExpr(from, this.compileContext));
    String name = fieldAccess.getField().getName();

    return ExprReadPropertyNodeGen.create(receiver, ExprLiteral.Object(name));
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

  @Override
  public ExprBase visitFrameSlotRef(RexFrameSlotRef rexFrameSlotRef) {
    int index = rexFrameSlotRef.getIndex();
    FrameSlot slot = from.findFrameSlot(index);
    Objects.requireNonNull(slot);

    return ExprReadLocalNodeGen.create(slot);
  }
}
