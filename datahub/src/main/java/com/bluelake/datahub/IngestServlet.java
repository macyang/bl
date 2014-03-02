package com.bluelake.datahub;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.channels.Channels;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONException;
import org.json.JSONObject;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.DatastoreServiceFactory;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsInputChannel;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.cloudstorage.RetryParams;
import com.bluelake.datahub.util.GcsClassLoader;
import com.bluelake.datahub.udf.IngestProcessor;

public class IngestServlet extends HttpServlet {
  static final long serialVersionUID = 1234567890l;
  private static final Logger LOG = Logger.getLogger(IngestServlet.class.getName());

  public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    String bucketName = req.getParameter(DataHubConstants.INPUT_REQUEST_PARAM_BUCKET);
    String gcsObjectName = req.getParameter(DataHubConstants.INGEST_REQUEST_PARAM_GCSOBJ);
    String entityKey = req.getParameter(DataHubConstants.INPUT_REQUEST_PARAM_ENTITYKEY);
    String startPositionS =
        req.getParameter(DataHubConstants.INGEST_REQUEST_PARAM_STARTINDEX);
    String pageSizeS = req.getParameter(DataHubConstants.INGEST_REQUEST_PARAM_PAGESIZE);

    if (startPositionS != null) {
      long startPosition;
      long pageSize;

      try {
        if (pageSizeS != null) {
          pageSize = Long.parseLong(pageSizeS);
        } else {
          pageSize = DataHubConstants.GCS_SPLITSIZE;
        }
        startPosition = Long.parseLong(startPositionS);
        ingestGCSToDS(bucketName, gcsObjectName, entityKey, startPosition, pageSize);
        resp.setStatus(HttpServletResponse.SC_OK);
      } catch (NumberFormatException e) {
        LOG.log(Level.SEVERE, e.getMessage(), e);
        resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      }
    }
  }

  private void ingestGCSToDS(String bucketName, String gcsObjectName, String entityKey,
      long startPosition, long pageSize) throws IOException {
    LOG.log(Level.INFO, "start processing");
    LOG.log(Level.INFO, "gcs object: " + bucketName + "/" + gcsObjectName);
    LOG.log(Level.INFO, "startPosition: " + startPosition);

    if (bucketName == null) {
      bucketName = "mac-test";
    }

    if (gcsObjectName == null) {
      gcsObjectName = "bar";
    }

    if (entityKey == null) {
      entityKey = "imei";
    }
    String[] entityKeyArray = entityKey.split(",");

    if (gcsObjectName != null) {

      // load the user defined IngestProcessor
      IngestProcessor ingestProcessor;
      try {
        GcsClassLoader loader =
            new GcsClassLoader(this.getClass().getClassLoader(), "mac-test", "test.jar");
/*
        LOG.log(Level.INFO, "trying out the urlclassloader");
        URLClassLoader loader = new URLClassLoader(new URL[] {new URL("https://storage.cloud.google.com/mac-test/test.jar")});
        */
        Class<? extends IngestProcessor> cz =
            loader.loadClass("com.bluelake.datahub.test.TestIngestProcessor").asSubclass(
                IngestProcessor.class);
        ingestProcessor = cz.newInstance();
      } catch (Exception e) {
        LOG.log(Level.SEVERE, "failed to load class, " + e.getMessage(), e);
        ingestProcessor = new IngestProcessor();
      }

      GcsService gcsService =
          GcsServiceFactory.createGcsService(RetryParams.getDefaultInstance());
      DatastoreService datastoreService = DatastoreServiceFactory.getDatastoreService();

      GcsFilename fileName = new GcsFilename(bucketName, gcsObjectName);
      GcsInputChannel readChannel =
          gcsService.openPrefetchingReadChannel(fileName, 0, 1024 * 1024);
      BufferedReader br =
          new BufferedReader(new InputStreamReader(Channels.newInputStream(readChannel)));

      try {
        long entityCount = 1;
        String line = null;
        String entityId = "";

        while ((line = br.readLine()) != null && entityCount < (startPosition + pageSize)) {

          if (entityCount < startPosition) {
            entityCount++;
            continue;
          }

          LOG.log(Level.INFO, "processing: " + startPosition + "-" + entityCount);
          JSONObject jsonObj = new JSONObject(line);

          Entity entity = ingestProcessor.processRecord(entityKeyArray, jsonObj);

          datastoreService.put(entity);

          if (entityCount % 10000 == 0) {
            LOG.log(Level.INFO, "entityCount=" + entityCount);
          }
          entityCount++;
        }
        LOG.log(Level.INFO, "done processing");
      } catch (JSONException je) {
        LOG.log(Level.SEVERE, je.getMessage(), je);
      } finally {
        br.close();
      }
    }
  }
}
