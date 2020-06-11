package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.VirtualFrame;

abstract class ExprLiteral extends ExprBase {
  static ExprLiteral Boolean(boolean value) {
    return new ExprLiteral() {
      @Override
      Object executeGeneric(VirtualFrame frame) {
        return value;
      }

    };
  }

  static ExprLiteral Int(int value) {
    return new ExprLiteral() {
      @Override
      Object executeGeneric(VirtualFrame frame) {
        return value;
      }

    };
  }

  static ExprLiteral Long(long value) {
    return new ExprLiteral() {
      @Override
      Object executeGeneric(VirtualFrame frame) {
        return value;
      }

    };
  }

  static ExprLiteral Double(double value) {
    return new ExprLiteral() {
      @Override
      Object executeGeneric(VirtualFrame frame) {
        return value;
      }

    };
  }

  static ExprLiteral Object(Object value) {
    return new ExprLiteral() {
      @Override
      Object executeGeneric(VirtualFrame frame) {
        return value;
      }
    };
  }

  static ExprLiteral Null() {
    return new ExprLiteral() {
      @Override
      Object executeGeneric(VirtualFrame frame) {
        return SqlNull.INSTANCE;
      }

    };
  }
}
