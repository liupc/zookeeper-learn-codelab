package com.xiaomi.lpc.queue;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: liupengcheng@xiaomi.com
 */
public class Util {

  public static class IDUtil {

    private static AtomicInteger id = new AtomicInteger(0);

    /**
     * Generate unique id
     *
     * @return
     */
    public static String genUniqueId() {
      TimeBasedGenerator gen = Generators.timeBasedGenerator();
      return gen.generate().toString();
    }

    /**
     * Generate sequential id
     *
     * @return
     */
    public static String genSequentialId() {
      return String.valueOf(id.getAndIncrement());
    }
  }

  public static class Bytes {
    private static final Logger LOG = LoggerFactory.getLogger(Bytes.class);
    private static final int INT_SIZE = 4;
    private static final int LONG_SIZE = 8;
    private static final int BOOL_SIZE = 1;

    public static byte[] toBytes(int i) {
      return BigInteger.valueOf(i).toByteArray();
    }

    public static byte[] toBytes(long l) {
      return BigInteger.valueOf(l).toByteArray();
    }

    public static byte[] toBytes(String s) {
      return s == null ? null : s.getBytes();
    }

    public static <E extends Enum<E>> byte[] toBytes(E e) {
      return e == null ? null : e.name().getBytes();
    }

    public static byte[] toBytes(boolean b) {
      int i = b == true ? 1 : 0;
      return Bytes.toBytes(i);
    }

    public static String toString(byte[] b) {
      if (b == null)
        return null;
      else if (b.length == 0)
        return "";
      try {
        return new String(b, 0, b.length, "UTF-8");
      } catch (UnsupportedEncodingException e) {
        LOG.error("UTF-8 encoding not supported", e);
        return null;
      }
    }

    public static Integer toInteger(byte[] b) {
      if (b == null || b.length == 0 || b.length > INT_SIZE)
        throw new IllegalArgumentException("invalid argument");
      int r =0;
      for (int i=0; i<b.length; i++) {
        r <<= 8;
        r ^= b[i]&0xff;
      }
      return r;
    }

    public static Long toLong(byte[] b) {
      if (b == null || b.length == 0 || b.length > LONG_SIZE)
        throw new IllegalArgumentException("invalid argument");
      long r = 0;
      for (int i=0; i<b.length; i++) {
        r <<= 8;
        r ^= b[i]&0xff;
      }
      return r;
    }

    public static Boolean toBoolean(byte[] b) {
      if (b == null || b.length != BOOL_SIZE)
        throw new IllegalArgumentException("invalid argument");
      return (b[0]&0xff) == 1 ? true : false;
    }

  }
}
