package com.xiaomi.lpc.queue;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import org.apache.commons.lang.enums.Enum;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: liupengcheng@xiaomi.com
 */
// only suitable for one consumer and
public class Queue<V> implements Watcher {
  private static final Logger LOG = LoggerFactory.getLogger(Queue.class);

  private final String hostPort;
  private final String root;
  private ZooKeeper zk;
  private final byte[] mutex = new byte[0];
  private long offset;
  private long size;
  private Class dataType;

  public Queue(String hostPort, String name) throws IOException {
    this.hostPort = hostPort;
    this.root = name;
    this.offset = 0;
    this.zk = new ZooKeeper(this.hostPort, 3000, this);
    try {
      Stat stat = zk.exists(name, false);
      if (stat == null) {
        zk.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      }
    } catch (KeeperException e) {
      LOG.error("instantiating queue error", e);
    } catch (InterruptedException e) {
      LOG.error("zkClient interrupt error", e);
    }
  }

  /**
   * Put data into queue, no queue capacity is set,
   * so user should manage well heapsize and produce speed
   *
   * @param data
   */
  public synchronized void put(V data) {
    String id = Util.IDUtil.genSequentialId();
    System.out.println("put data #" + id);
    checkDataType(data);
    try {
      if (data instanceof Integer) {
        dataType = Integer.class;
        zk.create(root + "/" + id, Util.Bytes.toBytes((Integer) data), ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT);
        zk.exists(root + "/" + id, this);
      } else if (data instanceof Long) {
        dataType = Long.class;
        zk.create(root + "/" + id, Util.Bytes.toBytes((Long) data), ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT);
        zk.exists(root + "/" + id, this);
      } else if (data instanceof String) {
        dataType = String.class;
        zk.create(root + "/" + id, Util.Bytes.toBytes((String) data), ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT);
        zk.exists(root + "/" + id, this);
      } else if (data instanceof Boolean) {
        dataType = Boolean.class;
        zk.create(root + "/" + id, Util.Bytes.toBytes((Boolean) data), ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.PERSISTENT);
        zk.exists(root + "/" + id, this);
      } else {
        // can not reach
        throw new RuntimeException("invalid data type: data=" + data);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Get data from queue, it will block if no more data to read,
   * but it will be waken and continue consuming if new data coming.
   *
   * @return
   */
  public V get() {
    while (true) {
      Stat stat = null;
      long curOffset = offset;
      try {
        List<String> children = zk.getChildren(root, this);
        if (children == null || children.size() == 0) {
          synchronized (mutex) {
            // double check to avoid thread permanent wait caused by notify happens before wait,
            if (children == null || children.size() == 0)
              mutex.wait();
          }
        } else {
          System.out.println("get data #" + curOffset);
          byte[] data = zk.getData(root + "/" + curOffset, false, stat);
          if (data != null) {
            zk.delete(root + "/" + curOffset, 0);
            return decodeData(data, dataType);
          } else {
            // impossible
            throw new RuntimeException("get data is null");
          }
        }
      } catch (KeeperException e) {
        if (e instanceof KeeperException.NoNodeException) {
          LOG.error("get data error, data already consumed, offset=" + curOffset);
        }
      } catch (InterruptedException e) {

      }
    }
  }

  private V decodeData(byte[] data, Class<?> type) {
    String typeName = type.getSimpleName();
    Method method = null;
    try {
      method = Util.Bytes.class.getDeclaredMethod("to" + typeName, byte[].class);
      return (V) method.invoke(null, data);
    } catch (NoSuchMethodException e) {
      LOG.error("No such method: " + "to" + typeName, e);
      return null;
    } catch (InvocationTargetException e) {
      LOG.error("method invoke error:", e);
      return null;
    } catch (IllegalAccessException e) {
      LOG.error("illegal access:", e);
      return null;
    }
  }

  private void checkDataType(V data) {
    if (!(data instanceof Integer || data instanceof Long
        || data instanceof Boolean || data instanceof Enum)) {
      throw new IllegalArgumentException("invalid data type: data=" + data);
    }
  }

  public void process(WatchedEvent watchedEvent) {
    Event.EventType type = watchedEvent.getType();
    try {
      if (type == Event.EventType.NodeChildrenChanged) {
        List<String> children = zk.getChildren(root, this);
        if (children.size() > size) {
          size = children.size();
          synchronized (mutex) {
            mutex.notify();
          }
        } else {
          // if offset update in time, data may be consumed more than once
          // Thread.sleep(1000);
          System.out.println("inc offset by 1, current offset=" + offset);
          offset += 1;
          size = children.size();
        }
        System.out.println("queue size=" + size);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
