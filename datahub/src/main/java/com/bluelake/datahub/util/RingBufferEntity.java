package com.bluelake.datahub.util;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
import com.google.appengine.api.datastore.Transaction;

public class RingBufferEntity extends RingBuffer {
  private static final Logger LOG = Logger.getLogger(RingBufferEntity.class.getName());
  private static final int MAX_NUMSLOTS = 250;
  private DatastoreService datastoreService = DatastoreServiceFactory.getDatastoreService();
  private Entity entity;
  private String kind;
  private String key;
  private int size;
  private List<JSONObject> dataList = new ArrayList<JSONObject>();
  private Map<String, Object> cachedProperties = null;

  /**
   * Create a new RingBufferEntity and set it to the specified size. If the entity existed already
   * then just return the entity and the size parameter is ignored
   * 
   * @param kind
   * @param key
   * @param size
   */
  public RingBufferEntity(String kind, String key, int size) {
    if (size <= 0) {
      throw new IllegalArgumentException("Number of slots must be greater than zero (requested "
          + size + ")");
    }
    if (size > MAX_NUMSLOTS) {
      throw new IllegalArgumentException("Number of slots must be smaller than " + MAX_NUMSLOTS + " (requested "
          + size + ")");
    }
    this.kind = kind;
    this.key = key;
    this.size = size;
  }

  public void put() {
    int retries = 3;
    while (true) {
      Transaction txn = datastoreService.beginTransaction();
      try {
         entity = getOrCreateEntity(kind, key, size);
         // apply changes
         for (JSONObject data : dataList) {
           entity.setUnindexedProperty(formatId(nextSlot), new Text(data.toString()));
           nextSlot = nextId(nextSlot);
         }
         setConf(PROP_NEXTSLOT, String.valueOf(nextSlot));
         datastoreService.put(entity);
         txn.commit();
         break;
      } catch (ConcurrentModificationException cme) {
        if (retries == 0) {
          throw cme;
      }
      // Allow retry to occur
      --retries;
      }
    }
  }
  
  @Override
  public Iterator<JSONObject> iterator() {
    entity = getOrCreateEntity(kind, key, size);
    return new RingBufferIterator(previousId(nextSlot));
  }

  /*
   * This function is call from the RingBuffer iterator. It fetches all properties from the entity
   * and caches them locally so the iterator can provide a consistent view
   */
  @Override
  public JSONObject getDataAt(String id) {
    if (cachedProperties == null) {
      entity = getOrCreateEntity(kind, key, size);
      cachedProperties = entity.getProperties();
    }
    try {
      JSONObject result = new JSONObject((String) ((Text)cachedProperties.get(id)).getValue());
      return result;
    } catch (JSONException e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
      return null;
    }
  }

  @Override
  public void store(JSONObject data) {
    dataList.add(data);
  }

  /*
   * This is no-op since this class overwrites the store function to buffer the write operations.
   * The actual write happens in the put() method. (non-Javadoc)
   * 
   * @see com.bluelake.datamodule.RingBuffer#storeDataAt(java.lang.String, org.json.JSONObject)
   */
  @Override
  public void storeDataAt(String id, JSONObject data) {}

  @Override
  public String getConf(String key) {
    return (String) entity.getProperty(key);
  }

  @Override
  public void setConf(String k, String v) {
    entity.setUnindexedProperty(k, v);
  }

  @Override
  public boolean hasId(String id) {
    return entity.hasProperty(id);
  }

  private Entity getOrCreateEntity(String kind, String key, int size) {
    Entity entity;
    try {
      entity = datastoreService.get(KeyFactory.createKey(kind, key));
    } catch (EntityNotFoundException e) {
      entity = new Entity(kind, key);
      entity.setUnindexedProperty(PROP_NUMSLOTS, String.valueOf(size));
      entity.setUnindexedProperty(PROP_NEXTSLOT, String.valueOf(0));
    }
    numSlots = Integer.parseInt((String)entity.getProperty(PROP_NUMSLOTS));
    nextSlot = Integer.parseInt((String)entity.getProperty(PROP_NEXTSLOT));
    return entity;
  }

}
