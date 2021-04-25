package net.wrap_trap.truffle_arrow.language.parser.ast;

import lombok.Value;

import java.util.List;

public class AST {

  public interface ASTNode {}
  public interface Expression extends ASTNode {}

  @Value
  public static class IntValue implements Expression {
    double value;
  }

  public static IntValue intValue(double value) {
    return new IntValue(value);
  }

  @Value
  public static class StringValue implements Expression {
    String value;
  }

  public static StringValue stringValue(String value) {
    return new StringValue(value);
  }

  @Value
  public static class Variable implements Expression {
    String name;

    public String getVariableName() {
      return name.substring(1);
    }
  }

  @Value
  public static class BinaryOperator implements Expression {
    Expression left;
    Expression right;
    String op;
  }

  public static BinaryOperator binary(Expression left, Expression right, String op) {
    return new BinaryOperator(left, right, op);
  }

  public static Variable variable(String name) {
    return new Variable(name);
  }

  @Value
  public static class Command implements ASTNode {
    String command;
    Expression param;
  }

  public static Command command(String command, Expression param) {
    return new Command(command, param);
  }

  @Value
  public static class Assignment implements Expression {
    Variable variable;
    Expression expression;
  }

  public static Assignment assignment(Variable variable, Expression expression) {
    return new Assignment(variable, expression);
  }

  @Value
  public static class If implements ASTNode {
    Expression expression;
    List<ASTNode> statements;
  }

  public static If ifs(Expression expression, List<ASTNode> statements) {
    return new If(expression, statements);
  }
}
