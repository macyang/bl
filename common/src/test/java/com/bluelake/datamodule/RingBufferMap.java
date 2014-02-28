package com.bluelake.datamodule;

import java.util.HashMap;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

public class RingBufferMap extends RingBuffer {
  private Map<String, String> map = new HashMap<String, String>();
  
  public RingBufferMap(int size) {
    numSlots = size;
    nextSlot = 0;
    setProperty(PROP_NUMSLOTS, String.valueOf(numSlots));
    setProperty(PROP_NEXTSLOT, String.valueOf(nextSlot));
  }

  public Map<String, String> getImpl() {
    return map;
  }
  
  @Override
  public JSONObject getDataAt(String id) {
    try {
      JSONObject obj = new JSONObject(map.get(id));
      return obj;
    } catch (JSONException e) {
      System.out.println(e.getMessage());
      return null;
    }
  }
  
  @Override
  public void storeDataAt(String id, JSONObject data) {
    map.put(id, data.toString());
  }
  
  @Override
  public String getProperty(String k) {
    return (String)map.get(k);
  }

  @Override
  public void setProperty(String k, String v) {
    map.put(k, v);
  }

  @Override
  public boolean hasId(String id) {
    return map.containsKey(id);
  }

}
