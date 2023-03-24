package org.realcpf.bytesutil;

import java.nio.ByteBuffer;

public class ByteFinder {
  private final ByteBuffer buffer;
  private byte find;
  private int start;
  private final int end;

  public ByteFinder(ByteBuffer buffer, byte find) {
    this.buffer = buffer;
    this.find = find;
    this.buffer.position(0);
    this.start = buffer.position() - 1;
    this.end = this.start + buffer.remaining();
  }
  public void finder(byte find) {
    this.find = find;
  }
  public void restart(int i){
    this.buffer.position(i);
  }

  public void restart(){
    this.buffer.position(0);
  }

  public int lastIndex(){
    int last = 0;
    for (; start < end; ++start) {
      if (find == buffer.get(start)) {
        last = start;
      }
    }
    return last;
  }
  public int nextIndex() {

    for (start++; start <= end; ++start) {
      if (find == buffer.get(start)) {
        return start;
      }
    }
    return -1;
  }

}
