package com.xiaomi.lpc.queue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.xiaomi.lpc.util.ThreadWaiter;
import org.junit.Test;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: liupengcheng@xiaomi.com
 */
public class QueueTest {

  @Test
  public void testSingleConsumerAndSingleProducer() throws Exception {
    Queue<Integer> queue = new Queue<Integer>("127.0.0.1:2181", "/bucket");
    Thread producerThread = new Thread(new Producer(queue));
    Thread consumerThread = new Thread(new Consumer(queue));
    producerThread.start();
    consumerThread.start();

    new ThreadWaiter(Arrays.asList(new Thread[] {producerThread, consumerThread})).waitFor();
  }

  @Test
  public void testMultiProducerAndSingleConsumer() throws Exception {
    Queue<Integer> queue = new Queue<Integer>("127.0.0.1:2181", "/bucket");
    List<Thread> threads = new ArrayList<Thread>();
    Producer producer = new Producer(queue);
    for (int i=0; i<5; i++) {
      Thread p = new Thread(producer);
      threads.add(p);
      p.start();
    }
    Thread c = new Thread(new Consumer(queue));
    threads.add(c);
    c.start();

    new ThreadWaiter(threads).waitFor();
  }

  @Test
  public void testMutiProducerAndMultiConsumer() throws Exception {
    Queue<Integer> queue = new Queue<Integer>("127.0.0.1:2181", "/bucket");
    List<Thread> threads = new ArrayList<Thread>();
    Producer producer = new Producer(queue);
    for (int i=0; i<5; i++) {
      Thread p = new Thread(producer);
      threads.add(p);
      p.start();
    }
    Consumer consumer = new Consumer(queue);
    for (int i=0; i<5; i++) {
      Thread c = new Thread(consumer);
      threads.add(c);
      c.start();
    }

    new ThreadWaiter(threads).waitFor();
  }
}
