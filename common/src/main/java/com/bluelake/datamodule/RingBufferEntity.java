package com.bluelake.datamodule;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Text;

public class RingBufferEntity extends RingBuffer {
  private static final Logger LOG = Logger.getLogger(RingBufferEntity.class.getName());
  private DatastoreService datastoreService = DatastoreServiceFactory.getDatastoreService();
  private Entity entity;

  /**
   * Create a new RingBufferEntity and set it to the specified size. If the entity existed already
   * then just return the entity and the size parameter is ignored
   * 
   * @param kind
   * @param key
   * @param size
   */
  public RingBufferEntity(String kind, String key, int size) {
    try {
      entity = datastoreService.get(KeyFactory.createKey(kind, key));
      String s;
      if ((s = getProperty(PROP_NUMSLOTS)) != null) {
        numSlots = Integer.parseInt(s);
      }
      if ((s = getProperty(PROP_NEXTSLOT)) != null) {
        nextSlot = Integer.parseInt(s);
      }
    } catch (EntityNotFoundException e) {
      if (size <= 0) {
        throw new IllegalArgumentException(
            "Number of slots must be greater than zero (requested " + size + ")");
      }
      entity = new Entity(kind, key);
      numSlots = size;
      nextSlot = 0;
      setProperty(PROP_NUMSLOTS, String.valueOf(numSlots));
      setProperty(PROP_NEXTSLOT, String.valueOf(nextSlot));
    }
  }

  public Entity getImpl() {
    return entity;
  }

  public void put() {
    datastoreService.put(entity);
  }

  @Override
  public JSONObject getDataAt(String id) {
    try {
      JSONObject result = new JSONObject((String) entity.getProperty(id));
      return result;
    } catch (JSONException e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
      return null;
    }
  }

  @Override
  public void storeDataAt(String id, JSONObject data) {
    entity.setUnindexedProperty(id, new Text(data.toString()));
  }

  @Override
  public String getProperty(String key) {
    return (String) entity.getProperty(key);
  }

  @Override
  public void setProperty(String k, String v) {
    entity.setUnindexedProperty(k, v);
  }

  @Override
  public boolean hasId(String id) {
    return entity.hasProperty(id);
  }

}
