package com.xiaomi.lpc.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: liupengcheng@xiaomi.com
 */
public class Producer implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(Producer.class);
  private final Queue<Integer> queue;
  public Producer(Queue<Integer> queue) {
    this.queue = queue;
  }

  public void produce(int item) {
    queue.put(item);
  }

  public void run() {
    try {
      for (int i = 0; i < 10; i++) {
        produce(10 * i);
        System.out.println("Produce data " + 10*i);
        Thread.sleep(500);
      }
    } catch (InterruptedException e) {
      LOG.error("Producer interrupted", e);
    }
  }
}
