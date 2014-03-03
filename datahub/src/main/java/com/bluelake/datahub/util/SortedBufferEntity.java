package com.bluelake.datahub.util;

import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;
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

public class SortedBufferEntity implements Iterable<JSONObject> {
  private static final Logger LOG = Logger.getLogger(SortedBufferEntity.class.getName());
  private static final String PROP_NUMSLOTS = "__numslots";
  private static final int MAX_NUMSLOTS = 250;
  private static final int MAX_OVERSIZE = 16;
  private DatastoreService datastoreService = DatastoreServiceFactory.getDatastoreService();
  private Entity entity;
  private String kind;
  private String key;
  private int size;
  private int numSlots;
  private Map<String, JSONObject> dataMap = new HashMap<String, JSONObject>();
  private Map<String, Object> cachedProperties = null;

  /**
   * A SortedBufferEntity can hold a fixed number of properties.
   * The sorting is based on the nature order of the property names.
   * The SortedBufferEntity removes properties in ascending order to stay within its capacity.
   * The SortedBufferEntity iterator returns the data in descending order.
   */
  public SortedBufferEntity(String kind, String key, int size) {
    if (size <= 0) {
      throw new IllegalArgumentException("Number of slots must be greater than zero (requested "
          + size + ")");
    }
    if (size > MAX_NUMSLOTS) {
      throw new IllegalArgumentException("Number of slots must be smaller than " + MAX_NUMSLOTS
          + " (requested " + size + ")");
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

        /*
         * If the number of properties in the entity exceeds the threshold then trim it back down to
         * the proper size.
         */
        Map<String, Object> currentDataMap = entity.getProperties();
        int currentSize = currentDataMap.keySet().size();
        if (currentSize > (size + MAX_OVERSIZE)) {
          TreeSet<String> keySet = new TreeSet<String>(currentDataMap.keySet());
          for (int i = 0; i < (currentSize - size); i++) {
            entity.removeProperty(keySet.pollFirst());
          }
        }

        // apply changes
        for (String k : dataMap.keySet()) {
          JSONObject data = dataMap.get(k);
          entity.setUnindexedProperty(k, new Text(data.toString()));
        }

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
    return new SortedBufferIterator(entity.getProperties());
  }

  /*
   * This function is call from the RingBuffer iterator. It fetches all properties from the entity
   * and caches them locally so the iterator can provide a consistent view
   */
  public JSONObject getDataAt(String id) {
    if (cachedProperties == null) {
      entity = getOrCreateEntity(kind, key, size);
      cachedProperties = entity.getProperties();
    }
    try {
      JSONObject result = new JSONObject((String) ((Text) cachedProperties.get(id)).getValue());
      return result;
    } catch (JSONException e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
      return null;
    }
  }

  public void storeDataAt(String id, JSONObject data) {
    dataMap.put(id, data);
  }

  public String getConf(String key) {
    return (String) entity.getProperty(key);
  }

  public void setConf(String k, String v) {
    entity.setUnindexedProperty(k, v);
  }

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
    }
    numSlots = Integer.parseInt((String) entity.getProperty(PROP_NUMSLOTS));
    return entity;
  }

  class SortedBufferIterator implements Iterator<JSONObject> {

    Map<String, Object> map;
    TreeSet<String> keySet;
    Iterator<String> keyIterator;
    int count = 0;

    SortedBufferIterator(Map<String, Object> map) {
      this.map = map;
      keySet = new TreeSet<String>(map.keySet());
      // remove the properties that are used by SortedBufferEntity itself
      for (String k : keySet) {
        if (k.startsWith("__")) {
          keySet.remove(k);
        }
      }
      keyIterator = keySet.descendingIterator();
    }

    @Override
    public boolean hasNext() {
      if (count >= numSlots) {
        return false;
      } else {
        return keyIterator.hasNext();
      }
    }

    @Override
    public JSONObject next() {
      String key = keyIterator.next();
      JSONObject result = null;
      try {
        Text t = (Text)map.get(key);
        result = new JSONObject(t.getValue());
      } catch (JSONException e) {
        LOG.log(Level.SEVERE, e.getMessage(), e);
      }
      count++;
      return result;
    }

    @Override
    public void remove() {
      // no op
    }
  }

}
