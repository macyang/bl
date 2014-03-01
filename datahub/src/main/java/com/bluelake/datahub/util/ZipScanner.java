package com.bluelake.datahub.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsInputChannel;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.cloudstorage.RetryParams;

public class ZipScanner {
  private static final Logger LOG = Logger.getLogger(ZipScanner.class.getName());
  private static GcsService gcsService = GcsServiceFactory.createGcsService(RetryParams
      .getDefaultInstance());

  private ZipScanner() {}

  public static void scan(ZipInputStream zin, ZipEntryHandler h) throws IOException {
    ZipEntry entry;
    while ((entry = zin.getNextEntry()) != null) {
      if (!entry.isDirectory()) {
        h.readZipEntry(entry, zin);
      }
    }
  }

  public static byte[] readZipBytes(ZipEntry entry, ZipInputStream zis) throws IOException {
    int count;
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] buffer = new byte[1024];
    while ((count = zis.read(buffer)) != -1) {
        baos.write(buffer, 0, count);
    }
    byte[] bytes = baos.toByteArray();
    return bytes;
  }
  
  public static ZipInputStream getZipStream(String bucketName, String gcsObjectName)
      throws IOException {
    GcsFilename fileName = new GcsFilename(bucketName, gcsObjectName);
    GcsInputChannel readChannel = gcsService.openReadChannel(fileName, 0);
    InputStream in = Channels.newInputStream(readChannel);
    ZipInputStream zin = new ZipInputStream(in);
    return zin;
  }

}
