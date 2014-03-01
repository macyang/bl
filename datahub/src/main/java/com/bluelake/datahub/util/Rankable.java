package com.bluelake.datahub.util;

public interface Rankable extends Comparable<Rankable> {

  Object getObject();

  long getCount();
  
}
