package net.wrap_trap.truffle_arrow.truffle;

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.util.hash.ArrowBufHasher;
import org.apache.arrow.vector.BufferBacked;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.compare.VectorVisitor;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.ipc.message.ArrowFieldNode;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.CallBack;
import org.apache.arrow.vector.util.TransferPair;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;

abstract class FieldVectorProxy implements FieldVector {

  private FieldVector inner;

  public FieldVectorProxy(FieldVector inner) {
    this.inner = inner;
  }

  @Override
  public void initializeChildrenFromFields(List<Field> list) {
    inner.initializeChildrenFromFields(list);
  }

  @Override
  public List<FieldVector> getChildrenFromFields() {
    return inner.getChildrenFromFields();
  }

  @Override
  public void loadFieldBuffers(ArrowFieldNode arrowFieldNode, List<ArrowBuf> list) {
    inner.loadFieldBuffers(arrowFieldNode, list);
  }

  @Override
  public List<ArrowBuf> getFieldBuffers() {
    return inner.getFieldBuffers();
  }

  @Override
  public List<BufferBacked> getFieldInnerVectors() {
    return inner.getFieldInnerVectors();
  }

  @Override
  public long getValidityBufferAddress() {
    return inner.getValidityBufferAddress();
  }

  @Override
  public long getDataBufferAddress() {
    return inner.getDataBufferAddress();
  }

  @Override
  public long getOffsetBufferAddress() {
    return inner.getOffsetBufferAddress();
  }

  @Override
  public void allocateNew() throws OutOfMemoryException {
    inner.allocateNew();
  }

  @Override
  public boolean allocateNewSafe() {
    return inner.allocateNewSafe();
  }

  @Override
  public void reAlloc() {
    inner.reAlloc();
  }

  @Override
  public BufferAllocator getAllocator() {
    return inner.getAllocator();
  }

  @Override
  public void setInitialCapacity(int i) {
    inner.setInitialCapacity(i);
  }

  @Override
  public int getValueCapacity() {
    return inner.getValueCapacity();
  }

  @Override
  public void close() {
    inner.close();
  }

  @Override
  public void clear() {
    inner.clear();
  }

  @Override
  public void reset() {
    inner.reset();
  }

  @Override
  public Field getField() {
    return inner.getField();
  }

  @Override
  public Types.MinorType getMinorType() {
    return inner.getMinorType();
  }

  @Override
  public TransferPair getTransferPair(BufferAllocator bufferAllocator) {
    return inner.getTransferPair(bufferAllocator);
  }

  @Override
  public TransferPair getTransferPair(String s, BufferAllocator bufferAllocator) {
    return inner.getTransferPair(s, bufferAllocator);
  }

  @Override
  public TransferPair getTransferPair(String s, BufferAllocator bufferAllocator, CallBack callBack) {
    return inner.getTransferPair(s, bufferAllocator, callBack);
  }

  @Override
  public TransferPair makeTransferPair(ValueVector valueVector) {
    return inner.makeTransferPair(valueVector);
  }

  @Override
  public FieldReader getReader() {
    return inner.getReader();
  }

  @Override
  public int getBufferSize() {
    return inner.getBufferSize();
  }

  @Override
  public int getBufferSizeFor(int i) {
    return inner.getBufferSizeFor(i);
  }

  @Override
  public ArrowBuf[] getBuffers(boolean b) {
    return inner.getBuffers(b);
  }

  @Override
  public ArrowBuf getValidityBuffer() {
    return inner.getValidityBuffer();
  }

  @Override
  public ArrowBuf getDataBuffer() {
    return inner.getDataBuffer();
  }

  @Override
  public ArrowBuf getOffsetBuffer() {
    return inner.getOffsetBuffer();
  }

  @Override
  public int getValueCount() {
    return inner.getValueCount();
  }

  @Override
  public void setValueCount(int i) {
    inner.setValueCount(i);
  }

  @Override
  public int getNullCount() {
    return inner.getNullCount();
  }

  @Override
  public boolean isNull(int i) {
    return inner.isNull(i);
  }

  @Override
  public int hashCode(int i) {
    return inner.hashCode(i);
  }

  @Override
  public int hashCode(int i, ArrowBufHasher arrowBufHasher) {
    return inner.hashCode(i, arrowBufHasher);
  }

  @Override
  public void copyFrom(int i, int i1, ValueVector valueVector) {
    inner.copyFrom(i, i1, valueVector);
  }

  @Override
  public void copyFromSafe(int i, int i1, ValueVector valueVector) {
    inner.copyFromSafe(i, i1, valueVector);
  }

  @Override
  public <OUT, IN> OUT accept(VectorVisitor<OUT, IN> vectorVisitor, IN in) {
    return inner.accept(vectorVisitor, in);
  }

  @Override
  public String getName() {
    return inner.getName();
  }

  @NotNull
  @Override
  public Iterator<ValueVector> iterator() {
    return inner.iterator();
  }

  @Override
  public void forEach(Consumer<? super ValueVector> action) {
    inner.forEach(action);
  }

  @Override
  public Spliterator<ValueVector> spliterator() {
    return inner.spliterator();
  }
}
