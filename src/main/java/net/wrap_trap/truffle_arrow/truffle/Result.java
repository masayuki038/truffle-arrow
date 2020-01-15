package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.InvalidArrayIndexException;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.interop.UnsupportedMessageException;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;

import java.util.List;
@ExportLibrary(InteropLibrary.class)
public class Result implements TruffleObject {

 final List<Object[]> result;

  public Result(List<Object[]> result) {
    this.result = result;
  }

  @ExportMessage
  boolean hasArrayElements() {
    return true;
  }

  @ExportMessage
  @CompilerDirectives.TruffleBoundary
  Object readArrayElement(long index) throws UnsupportedMessageException, InvalidArrayIndexException {
    try {
      return this.result.get((int) index);
    } catch (IndexOutOfBoundsException ioob) {
      throw InvalidArrayIndexException.create(index);
    }
  }

  @ExportMessage
  @CompilerDirectives.TruffleBoundary
  Object writeArrayElement(long index, Object value) throws UnsupportedMessageException, InvalidArrayIndexException {
    throw UnsupportedMessageException.create();
  }

  @ExportMessage
  @CompilerDirectives.TruffleBoundary
  long getArraySize() {
    return this.result.size();
  }

  @ExportMessage(name = "isArrayElementReadable")
  @ExportMessage(name = "isArrayElementModifiable")
  @ExportMessage(name = "isArrayElementInsertable")
  boolean isArrayElementReadable(long index) {
    return index >= 0 && index < getArraySize();
  }
}
