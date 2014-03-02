package com.bluelake.myapp;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import com.bluelake.datahub.udf.DefaultTableSplitUdf;
import com.bluelake.datahub.util.RingBufferEntity;

public class MyProcessor extends DefaultTableSplitUdf {
  private static final Logger LOG = Logger.getLogger(MyProcessor.class.getName());

  @Override
  protected void processTableRow(JSONObject rowObj) throws JSONException {
    String imei = rowObj.getString("imei");
    String devicetime = rowObj.getString("devicetime");
    String event = rowObj.getString("event");
    RingBufferEntity ringBuffer = new RingBufferEntity(getEntityKind(), imei, 3);
    try {
      JSONObject obj = new JSONObject(event);
      obj.remove("ID");
      obj.put("xxxxxxx", devicetime);
      ringBuffer.store(obj);
      ringBuffer.put();
    } catch (JSONException je) {
      LOG.log(Level.SEVERE, je.getMessage(), je);
    }
  }
}