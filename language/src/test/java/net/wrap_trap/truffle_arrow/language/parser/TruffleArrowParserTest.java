package net.wrap_trap.truffle_arrow.language.parser;

import static net.wrap_trap.truffle_arrow.language.parser.ast.AST.*;
import org.jparsec.Parser;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

public class TruffleArrowParserTest {
  private final static String SAMPLE =
      "echo \"a\";\n" +
      "echo \"b\";\n" +
      "$a = 1;\n" +
      "if ($a == 0) echo $a;";

  @Test
  public void testIntValue() {
    Parser<IntValue> parser = TruffleArrowParser.integer().from(TruffleArrowParser.tokenizer, TruffleArrowParser.ignored);
    assertThat(parser.parse("123"), is(intValue(123)));
  }

  @Test
  public void testIdentifier() {
    Parser<String> parser = TruffleArrowParser.identifier().from(TruffleArrowParser.tokenizer, TruffleArrowParser.ignored);
    assertThat(parser.parse("abc"), is("abc"));
  }

  @Test
  public void testVariable() {
    Parser<Variable> parser = TruffleArrowParser.variable().from(TruffleArrowParser.tokenizer, TruffleArrowParser.ignored);
    assertThat(parser.parse("$hoge"), is(variable("$hoge")));
  }

  @Test
  public void testValue() {
    Parser<Expression> parser = TruffleArrowParser.value().from(TruffleArrowParser.tokenizer, TruffleArrowParser.ignored);
    assertThat(parser.parse("$hoge"), is(variable("$hoge")));
    assertThat(parser.parse("123"), is(intValue(123)));
  }

  @Test
  public void testOperator() {
    Parser<Expression> parser = TruffleArrowParser.operator().from(TruffleArrowParser.tokenizer, TruffleArrowParser.ignored);
    assertThat(parser.parse("12+3"), is(binary(intValue(12), intValue(3), "+")));
    assertThat(parser.parse("12+$a"), is(binary(intValue(12), variable("$a"), "+")));
    assertThat(parser.parse("$ab+123"), is(binary(variable("$ab"), intValue(123), "+")));
  }

  @Test
  public void testCommand() {
    Parser<Command> parser = parser(TruffleArrowParser.command());
    assertThat(parser.parse("echo 123"), is(command("echo", intValue(123))));
    assertThat(parser.parse("echo 123<3"), is(command("echo", binary(intValue(123), intValue(3), "<"))));
    assertThat(parser.parse("echo 23+3"), is(command("echo", binary(intValue(23), intValue(3), "+"))));
    assertThat(parser.parse("echo $a"), is(command("echo", variable("$a"))));
    assertThat(parser.parse("echo \"a\""), is(command("echo", stringValue("a"))));
  }

  @Test
  public void testAssignment() {
    Parser<Assignment> parser = parser(TruffleArrowParser.assignment());
    assertThat(parser.parse("$a=123"), is(assignment(variable("$a"), intValue(123))));
  }

  @Test
  public void testStatement() {
    Parser<ASTNode> parser = parser(TruffleArrowParser.statement());
    assertThat(parser.parse("$a;"), is(variable("$a")));
    assertThat(parser.parse("$a=$a+1;"), is(assignment(variable("$a"), binary(variable("$a"), intValue(1), "+"))));
    assertThat(parser.parse("echo \"aaa\";"), is(command("echo", stringValue("aaa"))));
  }

  @Test
  public void testIfs() {
    Parser<If> parser = parser(TruffleArrowParser.ifStatement());
    assertThat(
      parser.parse("if ($a < 3) echo $a;"), is(ifs(binary(variable("$a"), intValue(3), "<")
        , Arrays.asList(command("echo", variable("$a"))))));
  }

  @Test
  public void testIfsWithBlocks() {
    String ifs = "if ($a < 3) { \n"+
                 "  echo $a;\n" +
                 "  echo \"$a < 3\";\n" +
                 "}\n";

    Parser<If> parser = parser(TruffleArrowParser.ifStatement());
    assertThat(
      parser.parse(ifs), is(ifs(binary(variable("$a"), intValue(3), "<")
        , Arrays.asList(command("echo", variable("$a"))
          , command("echo", stringValue("$a < 3"))))));
  }

  @Test
  public void testScript() {
    Parser<List<ASTNode>> parser = parser(TruffleArrowParser.script());
    List<ASTNode> asts = parser.parse(SAMPLE);
    assertThat(asts.size(), is(4));
  }

  <T> Parser<T> parser(Parser<T> p) {
    return p.from(TruffleArrowParser.tokenizer, TruffleArrowParser.ignored);
  }
}
