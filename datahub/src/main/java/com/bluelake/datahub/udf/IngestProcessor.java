package com.bluelake.datahub.udf;

import org.json.JSONException;
import org.json.JSONObject;

import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Text;

public class IngestProcessor {

  public Entity processRecord(String[] entityKeyArray, JSONObject jsonObj) throws JSONException {
    String entityId = "";
    if (entityKeyArray != null) {
      for (String ek : entityKeyArray) {
        entityId += jsonObj.getString(ek);
      }
    }
    // use system generated entity Id if entityKeyArray is not provided
    // or if there is nothing in the data matching the specified entityKeyArray
    if ("".equals(entityId)) {
      entityId = null;
    }

    Entity entity;
    if (entityId == null) {
      entity = new Entity("entity");
    } else {
      entity = new Entity("entity", entityId);
    }
    for (String key : JSONObject.getNames(jsonObj)) {
      String value = jsonObj.getString(key);
      if (value != null && value.length() >= 500) {
        entity.setUnindexedProperty(key, new Text(value));
      } else {
        entity.setUnindexedProperty(key, value);
      }
    }
    return entity;
  }
}