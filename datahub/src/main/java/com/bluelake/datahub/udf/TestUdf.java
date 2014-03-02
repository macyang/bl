package com.bluelake.datahub.udf;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import com.bluelake.datahub.util.RingBufferEntity;

public class TestUdf extends DefaultTableSplitUdf {
  private static final Logger LOG = Logger.getLogger(TestUdf.class.getName());

  @Override
  protected void processTableRow(JSONObject rowObj) throws JSONException {
    String imei = rowObj.getString("imei");
    String devicetime = rowObj.getString("devicetime");
    String event = rowObj.getString("event");
    RingBufferEntity ringBuffer = new RingBufferEntity(getEntityKind(), imei, 10);
    try {
      JSONObject obj = new JSONObject(event);
      obj.remove("ID");
      // use a strange property name for devicetime
      obj.put("testudfdt", devicetime);
      ringBuffer.store(obj);
      ringBuffer.put();
    } catch (JSONException je) {
      LOG.log(Level.SEVERE, je.getMessage(), je);
    }
  }
}