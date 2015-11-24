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
public class ZkBarrier implements Watcher{
  private final String root;
  private final int size;
  private ZooKeeper zk;
  private static final byte[] mutex = new byte[0];
  private boolean shutdown;

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
    Event.EventType type = watchedEvent.getType();
    if (type == Event.EventType.None) {
      Event.KeeperState state = watchedEvent.getState();
      switch (state) {
        case AuthFailed:
        case Disconnected:
        case Expired:
          shutdown();
          return;
        default:
          return;
      }
    } else {
      synchronized (mutex) {
        mutex.notifyAll();
      }
    }
  }

  private void shutdown() {
    this.shutdown = true;
    mutex.notifyAll();
  }

  public boolean enter(String name, long delayTime) throws KeeperException, InterruptedException {
    Thread.sleep(delayTime);
    zk.create(root + "/" + name, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    while(true) {
      if (!shutdown) {
        synchronized (mutex) {
          List<String> children = zk.getChildren(root, this);
          if (children.size() < size)
            mutex.wait();
          else
            return true;
        }
      } else {
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
