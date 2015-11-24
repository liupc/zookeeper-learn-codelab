package com.xiaomi.lpc.barrier;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: liupengcheng@xiaomi.com
 */
public class ZkBarrierTest {

  static class Process implements Runnable {
    private final String name;
    private final int seq;
    private final ZkBarrier barrier;
    public Process (String name, int seq, ZkBarrier barrier) {
      this.name = name;
      this.seq = seq;
      this.barrier = barrier;
    }

    public void run() {
      try {
        if (barrier.enter(name, seq * 5000)) {
          System.out.println(name + " enter");
          barrier.leave(name);
          System.out.println(name + " leave");
        } else {
          System.out.println("shutdown...");
        }
      } catch (InterruptedException e) {
        System.out.println("shutdown due to thread interrupts.");
        e.printStackTrace();
      } catch (Exception e) {
        System.out.println("other error occured.");
        e.printStackTrace();
      }
    }
  }

  public static void main(String[] args) throws Exception {
    ZkBarrier barrier = new ZkBarrier("127.0.0.1:2181", "/test", 3);
    for (int i=0; i<3; i++) {
      Thread t = new Thread(new Process("Process-" + i, i, barrier));
      t.start();
    }
  }
}
