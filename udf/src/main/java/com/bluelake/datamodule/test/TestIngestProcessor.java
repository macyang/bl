package com.bluelake.datamodule.test;

import org.json.JSONException;
import org.json.JSONObject;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Text;

import com.bluelake.datamodule.processor.IngestProcessor;

public class TestIngestProcessor extends IngestProcessor {

  public Entity processRecord(String[] entityKeyArray, JSONObject jsonObj) throws JSONException {
    Entity entity;
    entity = new Entity("testentity");
    String value = jsonObj.getString("imei");
    entity.setProperty("testprop", "IMEI is " + value);
    return entity;
  }
}
