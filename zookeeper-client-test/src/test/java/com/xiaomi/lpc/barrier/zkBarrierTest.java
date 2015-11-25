package com.xiaomi.lpc.barrier;

import org.junit.Test;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: liupengcheng@xiaomi.com
 */
public class zkBarrierTest {

  @Test
  public void testSequentialEnter() throws Exception {
    for (int i=0; i<3; i++) {
      ZkBarrier barrier = new ZkBarrier("127.0.0.1:2181", "/test", 3);
      ZkBarrierTest.Process process = new ZkBarrierTest.Process("Process" + i, i, barrier);
      new Thread(process).start();
    }
    // junit will call System.exit() when main thread exit.
    // So just wait for some while
    synchronized (this) {
      wait(20 * 1000);
    }
  }

  @Test
  public void testSimultaneousEnter() throws Exception {
    for (int i=0; i<3; i++) {
      ZkBarrier barrier = new ZkBarrier("127.0.0.1:2181", "/test", 3);
      ZkBarrierTest.Process process = new ZkBarrierTest.Process("Process" + i, 0, barrier);
      new Thread(process).start();
    }
    synchronized (this) {
      wait(10 * 1000);
    }
  }
}
