package com.bluelake.datahub.udf;

import java.util.logging.Level;
import java.util.logging.Logger;

public final class UdfFactory {
  
  private static final Logger LOG = Logger.getLogger(UdfFactory.class.getName());
  
  public static void init(){
    
  }
  
  public static TableSplitUdf getTableSplitUdf(String className) {

    try {
      Class<? extends TableSplitUdf> implClass = Class.forName(className).asSubclass(TableSplitUdf.class);
      return (TableSplitUdf)implClass.newInstance();
    } catch (ClassNotFoundException e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
    } catch (InstantiationException e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
    } catch (IllegalAccessException e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
    }
    return null;
  }

}
