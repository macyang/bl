package com.bluelake.datamodule.util;

import java.util.Iterator;

import org.json.JSONObject;

public abstract class RingBuffer implements Iterable<JSONObject> {
  public static final String PROP_NUMSLOTS = "_numslots";
  public static final String PROP_NEXTSLOT = "_nextslot";
  public static final int DEFAULT_SIZE = 30;
  protected int numSlots = 0;
  protected int nextSlot = 0;
  
  protected RingBuffer() {
  }

  public void store(JSONObject data) {
    storeDataAt(formatId(nextSlot), data);
    nextSlot = nextId(nextSlot);
    // System.out.println("XXX nextSlot : " + nextSlot);
    setConf(PROP_NEXTSLOT, formatId(nextSlot));
  }

  @Override
  public Iterator<JSONObject> iterator() {
    return new RingBufferIterator(previousId(nextSlot));
  }
  
  public abstract JSONObject getDataAt(String id);
  public abstract void storeDataAt(String id, JSONObject data);
  public abstract String getConf(String k);
  public abstract void setConf(String k, String v);
  public abstract boolean hasId(String id);

  protected int nextId(int i) {
    return ((i + 1) % numSlots);
  }

  protected int previousId(int i) {
    return ((i + numSlots - 1) % numSlots);
  }

  protected String formatId(int i) {
    return String.format("%05d", i);
  }

  class RingBufferIterator implements Iterator<JSONObject> {
    int currentSlot;
    int count = 0;

    RingBufferIterator(int i) {
      currentSlot = i;
    }

    @Override
    public boolean hasNext() {
      // System.out.println("XXX hasNext : " + currentSlot);
      if (count >= numSlots) {
        return false;
      } else {
        return hasId(formatId(currentSlot));
      }
    }

    @Override
    public JSONObject next() {
      // System.out.println("XXX next : " + formatId(currentSlot));
      JSONObject result = getDataAt(formatId(currentSlot));
      currentSlot = previousId(currentSlot);
      count++;
      return result;
    }

    @Override
    public void remove() {
      // no op
    }
  }

}
