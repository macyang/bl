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

import com.bluelake.datahub.DataHubConstants;
import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.EntityNotFoundException;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.Text;
import com.google.appengine.api.datastore.Transaction;

public class SortedBufferEntity implements Iterable<JSONObject> {
  private static final Logger LOG = Logger.getLogger(SortedBufferEntity.class.getName());
  private static final String PROP_NUMSLOTS = "numslots";
  private static final int MAX_NUMSLOTS = 250;
  private static final int MAX_OVERSIZE = 0;
  private DatastoreService datastoreService = DatastoreServiceFactory.getDatastoreService();
  private Entity entity;
  private String kind;
  private String key;
  private int numSlots;
  private Map<String, JSONObject> dataMap = new HashMap<String, JSONObject>();
  private Map<String, Object> cachedProperties = null;
  private JSONObject confObj = new JSONObject();

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
    this.numSlots = size;

  }

  public void put() {
    int retries = 3;
    while (true) {
      Transaction txn = datastoreService.beginTransaction();
      try {
        getOrCreateEntity(kind, key, numSlots);

        /*
         * If the number of properties in the entity exceeds the threshold then trim it back down to
         * the proper size.
         */
        Map<String, Object> currentDataMap = entity.getProperties();
        int currentSize = currentDataMap.keySet().size();
        numSlots = confObj.getInt(PROP_NUMSLOTS);
        if (currentSize > (numSlots + MAX_OVERSIZE)) {
          TreeSet<String> keySet = new TreeSet<String>(currentDataMap.keySet());
          for (int i = 0; i < (currentSize - numSlots); i++) {
            entity.removeProperty(keySet.pollFirst());
LOG.log(Level.WARNING, "trim oldest entries");
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
      } catch (JSONException e) {
        LOG.log(Level.SEVERE, e.getMessage(), e);
      }
    }
  }

  @Override
  public Iterator<JSONObject> iterator() {
    getOrCreateEntity(kind, key, numSlots);
    return new SortedBufferIterator(entity.getProperties());
  }

  /*
   * This function is call from the RingBuffer iterator. It fetches all properties from the entity
   * and caches them locally so the iterator can provide a consistent view
   */
  public JSONObject getDataAt(String id) {
    if (cachedProperties == null) {
      getOrCreateEntity(kind, key, numSlots);
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

  public boolean hasId(String id) {
    return entity.hasProperty(id);
  }

  private void getOrCreateEntity(String kind, String key, int size) {
    
    try {
      entity = datastoreService.get(KeyFactory.createKey(kind, key));
      Text confText = (Text)entity.getProperty(DataHubConstants.PROP_CONF);
      if (confText != null) {
        confObj = new JSONObject(confText.getValue());
      }
      else {
        confObj = new JSONObject();
        try {
          confObj.put(DataHubConstants.ENTITY_KIND, this.getClass().getName());
          confObj.put(PROP_NUMSLOTS, size);
          numSlots = size;
        } catch (JSONException e1) {
          LOG.log(Level.SEVERE, e1.getMessage(), e1);
        }
      }
    } catch (EntityNotFoundException e) {
      entity = new Entity(kind, key);
      confObj = new JSONObject();
      try {
        confObj.put(DataHubConstants.ENTITY_KIND, this.getClass().getName());
        confObj.put(PROP_NUMSLOTS, size);
        numSlots = size;
      } catch (JSONException e1) {
        LOG.log(Level.SEVERE, e.getMessage(), e);
      }
    } catch (JSONException e) {
      // failed to get the config property from an existing entity
      LOG.log(Level.SEVERE, e.getMessage(), e);
    }
    entity.setUnindexedProperty(DataHubConstants.PROP_CONF, new Text(confObj.toString()));
    
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
        if (k.equals(DataHubConstants.PROP_CONF)) {
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
