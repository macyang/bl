package com.bluelake.datamodule;

import java.util.HashMap;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

import com.bluelake.datamodule.util.RingBuffer;

public class RingBufferMap extends RingBuffer {
  private Map<String, String> map = new HashMap<String, String>();
  
  public RingBufferMap(int size) {
    numSlots = size;
    nextSlot = 0;
    setConf(PROP_NUMSLOTS, String.valueOf(numSlots));
    setConf(PROP_NEXTSLOT, String.valueOf(nextSlot));
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
  public String getConf(String k) {
    return (String)map.get(k);
  }

  @Override
  public void setConf(String k, String v) {
    map.put(k, v);
  }

  @Override
  public boolean hasId(String id) {
    return map.containsKey(id);
  }

}
