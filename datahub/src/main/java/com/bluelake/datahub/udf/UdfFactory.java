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


// load the user defined IngestProcessor
//IngestProcessor ingestProcessor;
//try {
//GcsClassLoader loader =
//    new GcsClassLoader(this.getClass().getClassLoader(), "mac-test", "test.jar");
///*
// * LOG.log(Level.INFO, "trying out the urlclassloader"); URLClassLoader loader = new
// * URLClassLoader(new URL[] {new URL("https://storage.cloud.google.com/mac-test/test.jar")});
// */
//Class<? extends IngestProcessor> cz =
//    loader.loadClass("com.bluelake.datahub.test.TestIngestProcessor").asSubclass(
//        IngestProcessor.class);
//ingestProcessor = cz.newInstance();
//} catch (Exception e) {
//LOG.log(Level.SEVERE, "failed to load class, " + e.getMessage(), e);
//ingestProcessor = new IngestProcessor();
//}