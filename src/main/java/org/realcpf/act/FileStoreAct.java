package org.realcpf.act;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;


public final class FileStoreAct implements AutoCloseable {
  private final static Logger LOGGER = LoggerFactory.getLogger(FileStoreAct.class);

  @Override
  public void close() throws Exception {
    tool.close();
  }


  private static class MateKey {
    public MateKey(long start,long len){
      this.start = start;
      this.len = len;
    }
    long start;
    long len;
  }

  private static class FileChannelTool {
    private final long MAX_LEN = 10 * 1024 * 1024;
    private FileChannel keyFileChannel;
    private FileChannel dataFileChannel;
    private MappedByteBuffer keyFileMapped;
    private MappedByteBuffer dataFileMapped;
    private final byte FIND_NULL = (byte) 0x00;
    private final byte FIND_FIELD_SPLIT = (byte) ';';

    private static Map<String, MateKey> map;

    private FileChannelTool() {

      try {
        Path basePath = Path.of("");

        Path kPath = basePath.resolve("db.key");
        Path dPath = basePath.resolve("db.data");
        if (!Files.exists(kPath)) {
          Files.createFile(kPath);
          Files.createFile(dPath);
        }
        keyFileChannel = FileChannel.open(kPath, StandardOpenOption.WRITE, StandardOpenOption.READ);
        dataFileChannel = FileChannel.open(dPath, StandardOpenOption.WRITE, StandardOpenOption.READ);
        this.keyFileMapped = keyFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, MAX_LEN);
        this.dataFileMapped = dataFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, MAX_LEN);

        firstLoad();
      } catch (IOException e) {
        LOGGER.error("error when <init> FileStoreAct",e);
      }
    }

    private void firstLoad() {
      map = new HashMap<>();
      int index;
      int last = 0;
      int end = -1;

      while (keyFileMapped.hasRemaining()){
        if (keyFileMapped.get() == FIND_NULL) {
          end = keyFileMapped.position();
        }
      }
      if (end == -1) {
        return;
      }
      keyFileMapped.position(0);
      ByteBuffer buffer = keyFileMapped.slice(0,end);
      for (; ; ) {
        index =-1;
        while (buffer.hasRemaining()) {
          if (buffer.get() == FIND_FIELD_SPLIT) {
            index = buffer.position();
          }
        }
        if (index < 0) {
          break;
        }
        String line = buffer.slice(last,index).toString();
        String[] lines = line.split(":");
        long start = Long.parseLong(lines[0]);
        long len = Long.parseLong(lines[1]);
        String keyName = lines[2];
        LOGGER.info("load key from file {}",keyName);
        map.put(keyName,new MateKey(start,len));
//        byteBuf.readByte();
        last = index + 1;
      }
//      int di = dataByteBuf.forEachByte(ByteProcessor.FIND_NUL);
//      dataByteBuf.writerIndex(di == -1 ? 0 : di);
//      int ki = keyByteBuf.forEachByte(ByteProcessor.FIND_NUL);
//      keyByteBuf.writerIndex(ki == -1 ? 0 : ki);
    }

    private static final FileStoreAct act = new FileStoreAct();


    public void close() throws IOException {
      this.keyFileChannel.close();
      this.dataFileChannel.close();

    }

  }

  private final FileChannelTool tool;

  private FileStoreAct() {
    tool = new FileChannelTool();
  }

  public static FileStoreAct getInstance() {
    return FileChannelTool.act;
  }
}
