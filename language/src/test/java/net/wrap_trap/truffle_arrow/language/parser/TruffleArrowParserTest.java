package net.wrap_trap.truffle_arrow.language.parser;

import net.wrap_trap.truffle_arrow.language.parser.ast.AST;
import org.jparsec.Parser;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;


public class TruffleArrowParserTest {
  private final static String SAMPLE =
    "  echo \"a\";\n" +
      "  echo \"b\";\n";

  @Test
  public void testIntValue() {
    Parser<AST.IntValue> intParser = TruffleArrowParser.integer().from(TruffleArrowParser.tokenizer, TruffleArrowParser.ignored);
    assertThat(intParser.parse("123"), is(AST.intValue(123)));
  }

  @Test
  public void testIdentifier() {
    Parser<String> identifier = TruffleArrowParser.identifier().from(TruffleArrowParser.tokenizer, TruffleArrowParser.ignored);
    assertThat(identifier.parse("abc"), is("abc"));
  }

  @Test
  public void testVariable() {
    Parser<AST.Variable> parser = TruffleArrowParser.variable().from(TruffleArrowParser.tokenizer, TruffleArrowParser.ignored);
    assertThat(parser.parse("$hoge"), is(AST.variable("$hoge")));
  }

  @Test
  public void testValue() {
    Parser<AST.Expression> valueParser = TruffleArrowParser.value().from(TruffleArrowParser.tokenizer, TruffleArrowParser.ignored);
    assertThat(valueParser.parse("$hoge"), is(AST.variable("$hoge")));
    assertThat(valueParser.parse("123"), is(AST.intValue(123)));
  }

  @Test
  public void testOperator() {
    Parser<AST.Expression> op = TruffleArrowParser.operator().from(TruffleArrowParser.tokenizer, TruffleArrowParser.ignored);
    assertThat(op.parse("12+3"), is(AST.binary(AST.intValue(12), AST.intValue(3), "+")));
    assertThat(op.parse("12+$a"), is(AST.binary(AST.intValue(12), AST.variable("$a"), "+")));
    assertThat(op.parse("$ab+123"), is(AST.binary(AST.variable("$ab"), AST.intValue(123), "+")));
  }

  @Test
  public void testCommand() {
    Parser<AST.Command> comm = parser(TruffleArrowParser.command());
    assertThat(comm.parse("echo 123"), is(AST.command("echo", AST.intValue(123))));
    assertThat(comm.parse("echo 123<3"), is(AST.command("echo", AST.binary(AST.intValue(123), AST.intValue(3), "<"))));
    assertThat(comm.parse("echo 23+3"), is(AST.command("echo", AST.binary(AST.intValue(23), AST.intValue(3), "+"))));
    assertThat(comm.parse("echo $a"), is(AST.command("echo", AST.variable("$a"))));
    assertThat(comm.parse("echo \"a\""), is(AST.command("echo", AST.stringValue("a"))));
  }

  @Test
  public void testAssignment() {
    Parser<AST.Assignment> assignment = parser(TruffleArrowParser.assignment());
    assertThat(assignment.parse("$a=123"), is(AST.assignment(AST.variable("$a"), AST.intValue(123))));
  }

  @Test
  public void testStatement() {
    Parser<AST.ASTNode> statement = parser(TruffleArrowParser.statement());
    assertThat(statement.parse("$a;"), is(AST.variable("$a")));
    assertThat(statement.parse("$a=$a+1;"), is(AST.assignment(AST.variable("$a"), AST.binary(AST.variable("$a"), AST.intValue(1), "+"))));
    assertThat(statement.parse("echo \"aaa\";"), is(AST.command("echo", AST.stringValue("aaa"))));
  }

  @Test
  public void testIfs() {
    Parser<AST.If> ifs = parser(TruffleArrowParser.ifStatement());
    assertThat(
      ifs.parse("if ($a < 3) echo $a;"), is(AST.ifs(AST.binary(AST.variable("$a"), AST.intValue(3), "<")
        , Arrays.asList(AST.command("echo", AST.variable("$a"))))));
  }

  @Test
  public void testScript() {
    Parser<List<AST.ASTNode>> parser = parser(TruffleArrowParser.script());
    List<AST.ASTNode> asts = parser.parse(SAMPLE);
    assertThat(asts.size(), is(2));
  }

  <T> Parser<T> parser(Parser<T> p) {
    return p.from(TruffleArrowParser.tokenizer, TruffleArrowParser.ignored);
  }
}
