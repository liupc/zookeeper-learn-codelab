package com.xiaomi.lpc.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: liupengcheng@xiaomi.com
 */
public class Consumer implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);

  private final Queue<Integer> queue;
  public Consumer(Queue<Integer> queue) {
    this.queue = queue;
  }

  public int consume() {
    return queue.get();
  }
  public void run() {
    try {
      for (int i = 0; i < 10; i++) {
        int rs = consume();
        System.out.println("Consume data " + rs);
        Thread.sleep(800);
      }
    } catch (InterruptedException e) {
      LOG.error("Consumer interrupted", e);
    }
  }
}
