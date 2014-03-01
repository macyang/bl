package com.bluelake.datahub.udf;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import com.bluelake.datahub.util.RingBufferEntity;

public class TestUdf {
  private static final Logger LOG = Logger.getLogger(TestUdf.class.getName());

  public static void processRecord(String[] entityKeyArray, JSONObject jsonObj) throws JSONException {
    String imei = jsonObj.getString("imei");
    String devicetime = jsonObj.getString("devicetime");
    String event = jsonObj.getString("event");
    RingBufferEntity ringBuffer = new RingBufferEntity("batterysummary", imei, 10);
    try {
      JSONObject obj = new JSONObject(event);
      obj.remove("ID");
      obj.put("devicetime", devicetime);
      ringBuffer.store(obj);
      ringBuffer.put();
    } catch (JSONException je) {
      LOG.log(Level.SEVERE, je.getMessage(), je);
    }
  }
}