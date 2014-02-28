package com.bluelake.datamodule;

public interface Rankable extends Comparable<Rankable> {

  Object getObject();

  long getCount();
  
}
