package net.wrap_trap.truffle_arrow.truffle;

import net.wrap_trap.truffle_arrow.type.ArrowTimeSec;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.InvalidArrayIndexException;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.interop.UnsupportedMessageException;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.Text;


@ExportLibrary(InteropLibrary.class)
public class Row implements TruffleObject {

  final VectorSchemaRoot vectorSchemaRoot;
  final int rowIndex;

  public Row(int rowIndex, VectorSchemaRoot vectorSchemaRoot) {
    this.rowIndex = rowIndex;
    this.vectorSchemaRoot = vectorSchemaRoot;
  }

  @ExportMessage
  boolean hasArrayElements() {
    return true;
  }

  @ExportMessage
  @CompilerDirectives.TruffleBoundary
  Object readArrayElement(long index) {
    Object o = this.vectorSchemaRoot.getFieldVectors().get((int) index).getObject(this.rowIndex);
    if (o == null) {
      return SqlNull.INSTANCE;
    } else if (o instanceof Text) {
      return o.toString();
    } else if (o instanceof ArrowTimeSec) {
      return ((ArrowTimeSec) o).timeSec() * 1000;
    }
    return o;
  }

  @ExportMessage
  @CompilerDirectives.TruffleBoundary
  Object writeArrayElement(long index, Object value) throws UnsupportedMessageException, InvalidArrayIndexException {
    throw UnsupportedMessageException.create();
  }

  @ExportMessage
  @CompilerDirectives.TruffleBoundary
  long getArraySize() {
    return this.vectorSchemaRoot.getFieldVectors().size();
  }

  @ExportMessage(name = "isArrayElementReadable")
  @ExportMessage(name = "isArrayElementModifiable")
  @ExportMessage(name = "isArrayElementInsertable")
  boolean isArrayElementReadable(long index) {
    return index >= 0 && index < getArraySize();
  }
}
