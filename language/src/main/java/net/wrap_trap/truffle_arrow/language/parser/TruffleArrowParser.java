package net.wrap_trap.truffle_arrow.language.parser;

import net.wrap_trap.truffle_arrow.language.parser.ast.AST;
import org.jparsec.*;
import org.jparsec.pattern.CharPredicates;
import org.jparsec.pattern.Pattern;
import org.jparsec.pattern.Patterns;

import java.util.Arrays;
import java.util.List;


public class TruffleArrowParser {
  static String[] operators = {
    "<", ">", "+", "-", "(", ")", ";", "=", ",", "{", "}", "==", "."};
  static String[] keywords = {"echo", "if"};

  static Parser<Void> ignored = Scanners.WHITESPACES.optional();
  static Pattern varToken = Patterns.isChar('$').next(Patterns.isChar(CharPredicates.IS_ALPHA).many1());
  static Terminals terms = Terminals.operators(operators).words(Scanners.IDENTIFIER).keywords(keywords).build();

  static Parser<String> varParser = varToken.toScanner("variable").source();

  enum Tag {
    VARIABLE
  }

  public static final Parser<Tokens.Fragment> VAR_TOKENIZER =
    varParser.map(text -> Tokens.fragment(text, Tag.VARIABLE));

  public static final Parser<String> VAR_PARSER = Parsers.token(t -> {
    Object val = t.value();
    if (val instanceof Tokens.Fragment) {
      Tokens.Fragment c = (Tokens.Fragment) val;
      return Tag.VARIABLE.equals(c.tag()) ? c.text() : null;
    }
    return null;
  });

  static Parser<?> tokenizer = Parsers.or(
    terms.tokenizer(),
    Terminals.StringLiteral.DOUBLE_QUOTE_TOKENIZER,
    VAR_TOKENIZER,
    Terminals.IntegerLiteral.TOKENIZER,
    Terminals.Identifier.TOKENIZER);

  public static Parser<AST.IntValue> integer() {
    return Terminals.IntegerLiteral.PARSER.map(s -> AST.intValue(Double.parseDouble(s)));
  }

  public static Parser<AST.StringValue> string() {
    return Terminals.StringLiteral.PARSER.map(AST::stringValue);
  }

  public static Parser<AST.Variable> variable() {
    return VAR_PARSER.map(AST::variable);
  }

  public static Parser<AST.Expression> value() {
    return Parsers.or(integer(), assignment(), variable(), string(),
      terms.token("(").next(pr -> expression().followedBy(terms.token(")"))));
  }

  public static Parser<AST.Expression> operator() {
    return new OperatorTable<AST.Expression>()
             .infixl(terms.token(".").retn((l, r) -> AST.binary(l, r, ".")), 10)
             .infixl(terms.token("+").retn((l, r) -> AST.binary(l, r, "+")), 10)
             .infixl(terms.token("-").retn((l, r) -> AST.binary(l, r, "-")), 10)
             .build(value());
  }

  public static Parser<AST.Expression> bicond() {
    return operator().next(l ->
                             terms.token("==", "<", ">").source()
                               .next(op -> operator().map(r -> (AST.Expression) AST.binary(l, r, op.trim()))).optional(l));
  }

  public static Parser<AST.Expression> concat() {
    return bicond().next(l ->
                           terms.token(".").source()
                                .next(op -> bicond().map(r -> (AST.Expression) AST.binary(l, r, "."))).optional(l));
  }

  public static Parser<AST.Expression> expression() {
    return concat();
  }

  public static Parser<AST.Command> command() {
    return terms.token("echo")
             .next(t -> expression().map(exp -> AST.command(t.toString(), exp)));
  }

  public static Parser<AST.Assignment> assignment() {
    return variable().followedBy(terms.token("="))
             .next(v -> expression().map(exp -> AST.assignment(v, exp)));
  }

  public static Parser<String> identifier() {
    return Terminals.Identifier.PARSER;
  }

  public static Parser<AST.If> ifStatement() {
    return terms.token("if").next(t -> expression()
                                         .between(terms.token("("), terms.token(")"))
                                         .next(exp -> statements()
                                                        .map(statements -> AST.ifs(exp, statements))));
  }

  public static Parser<AST.ASTNode> statement() {
    return Parsers.or(Parsers.or(bicond(), command()).followedBy(terms.token(";")),
      ifStatement());
  }

  public static Parser<List<AST.ASTNode>> statements() {
    return Parsers.or(
      statement().map(s -> Arrays.asList(s)),
      statement().many().between(terms.token("{"), terms.token("}")));
  }

  public static Parser<List<AST.ASTNode>> script() {
    return statement().many();
  }

  public static Parser<List<AST.ASTNode>> createParser() {
    return script().from(tokenizer, ignored);
  }
}
