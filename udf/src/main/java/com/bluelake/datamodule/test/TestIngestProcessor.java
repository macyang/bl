package com.bluelake.datamodule.test;

import org.json.JSONException;
import org.json.JSONObject;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Text;

import com.bluelake.datamodule.processor.IngestProcessor;
import com.bluelake.datamodule.util.RingBufferEntity;

public class TestIngestProcessor extends IngestProcessor {

  public Entity processRecord(String[] entityKeyArray, JSONObject jsonObj) throws JSONException {
    Entity entity;
    String imei = jsonObj.getString("imei");
    RingBufferEntity = new RingBufferEntity("testentity", imei, 10);

    entity.setProperty("testprop", "IMEI is " + value);
    return entity;
  }
}
