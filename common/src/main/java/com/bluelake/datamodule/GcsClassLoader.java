package com.bluelake.datamodule;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsInputChannel;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.cloudstorage.RetryParams;

public class GcsClassLoader extends ClassLoader {
  private static final Logger LOG = Logger.getLogger(GcsClassLoader.class.getName());
  private Map<String, byte[]> byteStreams = new HashMap<String, byte[]>();

  public GcsClassLoader(ClassLoader parent, String bucketName, String gcsObjectName)
      throws IOException {
    super(parent);
    GcsService gcsService = GcsServiceFactory.createGcsService(RetryParams.getDefaultInstance());
    GcsFilename fileName = new GcsFilename(bucketName, gcsObjectName);
    GcsInputChannel readChannel = gcsService.openReadChannel(fileName, 0);
    InputStream is = Channels.newInputStream(readChannel);
    ZipInputStream zis = new ZipInputStream(is);
    addClassJar(zis);
    is.close();
  }

  public void addClassJar(ZipInputStream zis) throws IOException {
    ZipScanner.scan(zis, new ZipEntryHandler() {
      public void readZipEntry(ZipEntry entry, ZipInputStream in) throws IOException {
        String name = entry.getName();
        if (byteStreams.containsKey(name)) {
          LOG.log(Level.WARNING, "duplicate defintion of class/resource " + name);
        } else {
          LOG.log(Level.INFO, "adding class " + name);
          addClass(name, ZipScanner.readZipBytes(entry, in));
        }
      }
    });
  }

  void addClass(String className, byte[] data) throws IOException {
    byteStreams.put(className, data);
  }

  @Override
  protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    if (name == null) {
      throw new NullPointerException();
    }
    if (name.indexOf("/") != -1) {
      throw new ClassNotFoundException(name);
    }

    // Since all support classes of loaded class use same class loader
    // must check subclass cache of classes for things like Object
    Class<?> c = findLoadedClass(name);
    if (c == null) {
      try {
        c = getParent().loadClass(name);
      } catch (ClassNotFoundException ex) {
        // Load class data from file and save in byte array
        String fileName = name.replace('.', '/') + ".class";
        byte data[] = byteStreams.get(fileName);

        if (data == null) throw new ClassNotFoundException(name);

        // Convert byte array to Class
        c = defineClass(name, data, 0, data.length);

        // If failed, throw exception
        if (c == null) throw new ClassNotFoundException(name);
      }
    }

    // Resolve class definition if approrpriate
    if (resolve) resolveClass(c);

    // Return class just created
    return c;
  }

  @Override
  public InputStream getResourceAsStream(String name) {
    byte data[] = byteStreams.get(name);
    if (data == null) return null;
    return new ByteArrayInputStream(data);
  }

}
