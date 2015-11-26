package com.xiaomi.lpc.util;

import java.util.List;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: liupengcheng@xiaomi.com
 */
public class ThreadWaiter {

  private final long timeout;
  private final List<Thread> threads;

  public ThreadWaiter(long timeout, List<Thread> threads) {
    this.timeout = timeout;
    this.threads = threads;
  }

  public ThreadWaiter(List<Thread> threads) {
    this(-1, threads);
  }

  public void waitFor() throws InterruptedException {
    for (Thread t : threads) {
      if (timeout < 0)
        t.join();
      else
        t.join(this.timeout);
    }
  }
}
