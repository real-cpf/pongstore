package org.realcpf;

import org.realcpf.act.FileStoreAct;
import org.realcpf.struct.Tuple;

public class Main {
  public static void main(String[] args) {
    System.out.println("come soon!");
    FileStoreAct act = FileStoreAct.getInstance();
    Tuple<String,String> hello = new Tuple<>("hello","world");
//    act.putTuple(hello);
    try {
      act.close();
    } catch (Exception e) {
      e.printStackTrace();
    }

  }
}
