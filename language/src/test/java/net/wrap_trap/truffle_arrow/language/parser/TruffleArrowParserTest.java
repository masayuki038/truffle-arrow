package net.wrap_trap.truffle_arrow.language.parser;

import java.util.List;
import org.jparsec.Parser;
import org.junit.Test;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

public class TruffleArrowParserTest {
  private final static String SAMPLE =
      "  echo \"a\";\n" +
      "  echo \"b\";\n";

  @Test
  public void testScript() {
    Parser<List<TruffleArrowParser.AST>> parser = parser(TruffleArrowParser.script());
    List<TruffleArrowParser.AST> asts = parser.parse(SAMPLE);
    assertThat(asts.size(), is(2));
  }

  @Test
  public void test() {
    Parser<TruffleArrowParser.IntValue> intParser= TruffleArrowParser.integer().from(TruffleArrowParser.tokenizer, TruffleArrowParser.ignored);
    System.out.println(intParser.parse("123"));

    Parser<String> identifier = TruffleArrowParser.identifier().from(TruffleArrowParser.tokenizer, TruffleArrowParser.ignored);
    System.out.println(identifier.parse("abc"));

    Parser<TruffleArrowParser.ASTVariable> parser = TruffleArrowParser.variable().from(TruffleArrowParser.tokenizer, TruffleArrowParser.ignored);
    System.out.println(parser.parse("$hoge"));

    Parser<TruffleArrowParser.ASTExp> valueParser = TruffleArrowParser.value().from(TruffleArrowParser.tokenizer, TruffleArrowParser.ignored);
    System.out.println(valueParser.parse("$hoge"));
    System.out.println(valueParser.parse("123"));

    Parser<TruffleArrowParser.ASTExp> op = TruffleArrowParser.operator().from(TruffleArrowParser.tokenizer, TruffleArrowParser.ignored);
    System.out.println(op.parse("12+3"));
    System.out.println(op.parse("12+$a"));
    System.out.println(op.parse("$ab+123"));

    Parser<TruffleArrowParser.ASTCommand> comm = parser(TruffleArrowParser.command());
    System.out.println(comm.parse("echo 123"));
    System.out.println(comm.parse("echo 123<3"));
    System.out.println(comm.parse("echo 23+3"));
    System.out.println(comm.parse("echo $a"));
    System.out.println(comm.parse("echo \"a\""));

    Parser<String> ident = TruffleArrowParser.identifier().from(TruffleArrowParser.tokenizer, TruffleArrowParser.ignored);
    System.out.println(ident.parse("f"));

    Parser<TruffleArrowParser.ASTAssignment> assignment = parser(TruffleArrowParser.assignment());
    System.out.println(assignment.parse("$a=123"));

    Parser<TruffleArrowParser.AST> statement = parser(TruffleArrowParser.statement());
    System.out.println(statement.parse("$a;"));
    System.out.println(statement.parse("$a=$a+1;"));
    System.out.println(statement.parse("echo \"aaa\";"));

    Parser<TruffleArrowParser.ASTIf> ifs = parser(TruffleArrowParser.ifStatement());
    System.out.println(ifs.parse("if ($a < 3) echo $a;"));

    Parser<List<TruffleArrowParser.AST>> scriptParser = parser(TruffleArrowParser.script());
    final List<TruffleArrowParser.AST> script = scriptParser.parse(SAMPLE);
    System.out.println(script);
  }

  <T> Parser<T> parser(Parser<T> p) {
    return p.from(TruffleArrowParser.tokenizer, TruffleArrowParser.ignored);
  }

}
