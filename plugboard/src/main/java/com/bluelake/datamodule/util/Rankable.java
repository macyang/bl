package com.bluelake.datamodule.util;

public interface Rankable extends Comparable<Rankable> {

  Object getObject();

  long getCount();
  
}
