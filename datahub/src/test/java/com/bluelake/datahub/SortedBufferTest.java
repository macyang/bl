package com.bluelake.datahub;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;

import org.json.JSONException;
import org.json.JSONObject;

import com.bluelake.datahub.util.SortedBufferEntity;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalMemcacheServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;

public class SortedBufferTest {

  private final LocalServiceTestHelper helper = new LocalServiceTestHelper(
      new LocalDatastoreServiceTestConfig(), new LocalMemcacheServiceTestConfig());

  @Before
  public void setUp() {
    helper.setUp();
  }

  @After
  public void tearDown() {
    helper.tearDown();
  }

  @Test
  public void processEntryTest() throws InterruptedException {

    SortedBufferEntity sortedBuffer = new SortedBufferEntity("sortedbuffertest", "testkey", 3);
    try {
      for (int i = 0; i < 5; i++) {
        sortedBuffer.storeDataAt(String.valueOf(i), new JSONObject("{\"foo" + i + "\":\"bar" + i
            + "\"}"));
        sortedBuffer.put();
        printSortedBuffer(sortedBuffer);
      }
    } catch (JSONException e) {
      System.out.println(e.getMessage());
    }
  }

  void printSortedBuffer(SortedBufferEntity sortedBuffer) {
    System.out.println("\n========================");
    for (JSONObject j : sortedBuffer) {
      System.out.println(j.toString());
    }
    System.out.println("========================");
  }

}
