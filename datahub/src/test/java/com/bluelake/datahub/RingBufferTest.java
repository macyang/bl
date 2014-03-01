package com.bluelake.datahub;

import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;

import org.json.JSONException;
import org.json.JSONObject;

import com.bluelake.datahub.util.RingBuffer;

public class RingBufferTest {

  @Before
  public void setUp() {}

  @After
  public void tearDown() {}

  @Test
  public void processEntryTest() throws InterruptedException {

    RingBufferMap ringBuffer = new RingBufferMap(3);
    try {
      for (int i = 0; i < 5; i++) {
        ringBuffer.store(new JSONObject("{\"foo" + i + "\":\"bar" + i + "\"}"));
        printRingBuffer(ringBuffer);
        // printMap(container.getMap());
      }
    } catch (JSONException e) {
      System.out.println(e.getMessage());
    }
  }

  void printRingBuffer(RingBuffer ringBuffer) {
    System.out.println("\n========================");
    for (JSONObject j : ringBuffer) {
      System.out.println(j.toString());
    }
    System.out.println("========================");
  }

  void printMap(Map<String, String> m) {
    System.out.println("\n========================");
    for (String k : m.keySet()) {
      System.out.println(k + " : " + m.get(k));
    }
    System.out.println("========================");
  }
}
