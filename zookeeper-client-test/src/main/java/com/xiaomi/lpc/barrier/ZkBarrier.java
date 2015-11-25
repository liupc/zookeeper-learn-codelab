package com.xiaomi.lpc.barrier;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: liupengcheng@xiaomi.com
 */
public class ZkBarrier implements Watcher {
  private final String root;
  private final int size;
  private ZooKeeper zk;
  private static final byte[] mutex = new byte[0];
  private boolean shutdown;

  /**
   * Barrier is to make all workers in a group begin to work simultaneously,
   * and can only leave after all workers finish their work.
   *
   * @param hostPort host:port like string indicating zookeeper server addr
   * @param root     root znode
   * @param size     group scale
   */
  public ZkBarrier(String hostPort, String root, int size) {
    this.root = root;
    this.size = size;
    this.shutdown = false;

    try {
      this.zk = new ZooKeeper(hostPort, 3000, this);
      Stat s = zk.exists(root, false);
      if (s == null) {
        zk.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public void process(WatchedEvent watchedEvent) {
    System.out.println(Thread.currentThread().getName() + " watcher are notified.");
    Event.EventType type = watchedEvent.getType();
    if (type == Event.EventType.None) {
      Event.KeeperState state = watchedEvent.getState();
      switch (state) {
        case AuthFailed:
        case Disconnected:
        case Expired:
          // in case of zookeeper connection failed(auth failure, disconnected, expired.)
          shutdown();
          return;
        default:
          return;
      }
    } else {
      synchronized (mutex) {
        // notify all waiter thread on mutex, only one waiter for each mutex in barrier
        mutex.notify();
      }
    }
  }

  private void shutdown() {
    this.shutdown = true;
    mutex.notify();
  }

  /**
   * @param name      name of child znode to create under root znode
   * @param delayTime milliseconds to delay before enter
   * @return
   * @throws KeeperException
   * @throws InterruptedException
   */
  public boolean enter(String name, long delayTime) throws KeeperException, InterruptedException {
    Thread.sleep(delayTime);
    zk.create(root + "/" + name, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    while (true) {
      if (!shutdown) {
        synchronized (mutex) {
          List<String> children = zk.getChildren(root, this);
          if (children.size() < size) {
            mutex.wait();
          } else {
            return true;
          }
        }
      } else {
        // return false when shutdown is true
        return false;
      }
    }
  }

  public boolean leave(String name) throws KeeperException, InterruptedException {
    zk.delete(root + "/" + name, 0);
    while (true) {
      if (!shutdown) {
        synchronized (mutex) {
          List<String> children = zk.getChildren(root, this);
          if (children.size() > 0) {
            mutex.wait();
          } else {
            return true;
          }
        }
      }
    }
  }
}
