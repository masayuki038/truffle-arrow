package net.wrap_trap.truffle_arrow;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;


public class ArrowUtils {

  private static final RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);

  public static UInt4Vector createSelectionVector() {
    return new UInt4Vector("selectionVector", allocator);
  }

  public static VectorSchemaRoot[] load(String path) throws IOException {
    byte[] bytes = Files.readAllBytes(FileSystems.getDefault().getPath(path));
    SeekableReadChannel channel = new SeekableReadChannel(new ByteArrayReadableSeekableByteChannel(bytes));
    ArrowFileReader reader = new ArrowFileReader(channel, allocator);
    List<VectorSchemaRoot> list = reader.getRecordBlocks().stream().map(block -> {
      try {
        if (!reader.loadRecordBatch(block)) {
          throw new IllegalStateException("Failed to load RecordBatch");
        }
        return reader.getVectorSchemaRoot();
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }).collect(Collectors.toList());
    VectorSchemaRoot[] vectorSchemaRoots = new VectorSchemaRoot[list.size()];
    list.toArray(vectorSchemaRoots);
    return vectorSchemaRoots;
  }
}
