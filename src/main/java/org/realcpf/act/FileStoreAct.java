package org.realcpf.act;


import org.realcpf.bytesutil.ByteFinder;
import org.realcpf.struct.Tuple;
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

  public int putTuple(Tuple<String, String> tuple) {
    return this.tool.putTuple(tuple);
  }
  public String getTuple(String key){
    return this.tool.getTuple(key);
  }

  private static class MateKey {
    public MateKey(long start, long len) {
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
    private final byte FIND_NULL = (byte) 0x0000;
    private final byte FIND_FIELD_SPLIT = (byte) ';';
    private final byte FIND_KEY_SPLIT = (byte) ':';

    private static Map<String, MateKey> map;

    private FileChannelTool() {

      try {
        Path basePath = Path.of(System.getenv("DATA_PATH"), "db");

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
        LOGGER.error("error when <init> FileStoreAct", e);
      }
    }

    private void firstLoad() {
      map = new HashMap<>();
      int index;
      int last = 0;


      ByteFinder finder = new ByteFinder(keyFileMapped, FIND_FIELD_SPLIT);
      int tmp = -1;
      int end = 0;
      while ((tmp = finder.nextIndex()) != -1) {
        end = tmp;
      }
      if (end < 1) {
        return;
      }
      int ki = end;
      keyFileMapped.position(0);
      ByteBuffer buffer = keyFileMapped.slice(0, end + 1);
      ByteFinder fieldFinder = new ByteFinder(buffer, FIND_FIELD_SPLIT);
      fieldFinder.restart();
      for (; ; ) {
        index = fieldFinder.nextIndex();
        if (index < 0) {
          break;
        }

        ByteBuffer line = buffer.slice(last, index + 1);
        long start = line.getLong();
        line.get();
        long len = line.getLong();
        line.get();

        ByteBuffer keyBuffer = line.slice(line.position(), line.remaining()-1);
        line.get();
        byte[] bbs = new byte[keyBuffer.remaining()];
        keyBuffer.get(bbs);
        String keyName = new String(bbs);
        map.put(keyName,new MateKey(start,len));
        LOGGER.info("load key from file {}", keyName);
        buffer.get();
        last = index + 1;
      }

      ByteFinder dataFinder = new ByteFinder(dataFileMapped, FIND_NULL);
      int di = dataFinder.nextIndex();
      dataFileMapped.position(di);
      keyFileMapped.position(ki);
    }

    private static final FileStoreAct act = new FileStoreAct();


    public void close() throws IOException {
      this.keyFileChannel.close();
      this.dataFileChannel.close();

    }
    private String getTuple(String key){
      if (map.containsKey(key)) {
        MateKey mateKey = map.get(key);
        synchronized (FileChannelTool.class){
          int start = (int)mateKey.start;
          int len = (int)mateKey.len;
          byte[] value = new byte[len];
          dataFileMapped.get(start,value);
          return new String(value);
        }
      }
      return null;
    }

    private int putTuple(Tuple<String, String> tuple) {
      String key = tuple.getKey();
      String value = tuple.getValue();
      byte[] keys = key.getBytes(StandardCharsets.UTF_8);
      byte[] values = value.getBytes(StandardCharsets.UTF_8);

      // index:length:key-bytes
      int nowIndex = dataFileMapped.position();
      int bodyLen = values.length;
      map.put(key,new MateKey(nowIndex,bodyLen));
      synchronized (FileChannelTool.class) {
        keyFileMapped.putLong(nowIndex);
        keyFileMapped.put(FIND_KEY_SPLIT);
        keyFileMapped.putLong(bodyLen);
        keyFileMapped.put(FIND_KEY_SPLIT);
        keyFileMapped.put(keys);
        keyFileMapped.put(FIND_FIELD_SPLIT);
        dataFileMapped.put(values);
      }
      return 1;
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
