package org.realcpf.struct;

public class Tuple<A,B> {
  public A getKey() {
    return key;
  }

  public B getValue() {
    return value;
  }

  private final A key;
  private final B value;
  public Tuple(A key,B value){
    this.key = key;
    this.value = value;
  }
}
