package com.xiaomi.lpc.barrier;

import java.util.Random;

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

    /**
     * constructor of runnable
     * @param name name of Process, which will be used to create child znode
     * @param seq sequence id of Process
     * @param barrier zkBarrier object
     */
    public Process (String name, int seq, ZkBarrier barrier) {
      this.name = name;
      this.seq = seq;
      this.barrier = barrier;
    }

    public void run() {
      try {
        // set delay time ot seq * 5000 to make zk notification processing
        // once at a time
        if (barrier.enter(name, seq * 5000)) {
          System.out.println(name + " enter");
          // make sure all threads entered processing state
          Thread.sleep(1000 + new Random().nextInt(2000));
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
    for (int i=0; i<3; i++) {
      // each Thread contains a barrier and manage their own zkClient,
      // when changes happen in znode, all zkClient watching this znode will be notified.
      ZkBarrier barrier = new ZkBarrier("127.0.0.1:2181", "/test", 3);
      Thread t = new Thread(new Process("Process-" + i, i, barrier));
      t.setName("Thread" + i);
      t.start();
    }
  }
}
