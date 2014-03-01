package com.bluelake.datamodule;

import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.Label;
import com.google.api.services.pubsub.model.PublishRequest;
import com.google.api.services.pubsub.model.PubsubMessage;
import com.google.api.services.pubsub.model.Topic;
import com.google.appengine.api.backends.BackendServiceFactory;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.appengine.tools.cloudstorage.GcsInputChannel;
import com.google.appengine.tools.cloudstorage.GcsService;
import com.google.appengine.tools.cloudstorage.GcsServiceFactory;
import com.google.appengine.tools.cloudstorage.RetryParams;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class GcsInputRequestServlet extends HttpServlet {
  static final long serialVersionUID = 1234567890l;
  private static final Logger LOG = Logger.getLogger(GcsInputRequestServlet.class.getName());

  public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    String bucketName = req.getParameter(DataModuleConstants.INPUT_REQUEST_PARAM_BUCKET);
    String gcsObjectNames = req.getParameter(DataModuleConstants.INPUT_REQUEST_PARAM_GCSOBJS);
    String entityKey = req.getParameter(DataModuleConstants.INPUT_REQUEST_PARAM_ENTITYKEY);

    /*
     * Provide default values for input parameters
     */
    if (bucketName == null) {
      bucketName = "mac-test";
    }
    if (gcsObjectNames == null) {
      gcsObjectNames = "bar";
    }
    if (entityKey == null) {
      entityKey = "imei";
    }
    LOG.log(Level.INFO, "gcsobjectNames: " + gcsObjectNames);
    requestGCSToDS(bucketName, gcsObjectNames, entityKey);
    resp.setStatus(HttpServletResponse.SC_OK);
  }

  private void requestGCSToDS(String bucketName, String gcsObjectNames, String entityKey)
      throws IOException {
    LOG.log(Level.INFO, "process input request");
    GcsService gcsService = GcsServiceFactory.createGcsService(RetryParams.getDefaultInstance());

    if (gcsObjectNames != null) {
      for (String gcsObjectName : gcsObjectNames.split(",")) {
        long entityCount = 0l;
        GcsFilename fileName = new GcsFilename(bucketName, gcsObjectName);
        GcsInputChannel readChannel =
            gcsService.openPrefetchingReadChannel(fileName, 0, 1024 * 1024);
        BufferedReader br =
            new BufferedReader(new InputStreamReader(Channels.newInputStream(readChannel)));

        try {
          LOG.log(Level.INFO, "split: " + bucketName + "/" + gcsObjectName);
          // scan the file to count the number of records
          while (br.readLine() != null) {
            entityCount++;
          }
        } catch (IOException ioe) {
          LOG.log(Level.SEVERE, ioe.getMessage(), ioe);
        } finally {
          br.close();
        }

        if (entityCount > 0) {
          // create one task for every GCS_SPLITSIZE number of records
          for (long l = 1; l <= entityCount; l += DataModuleConstants.GCS_SPLITSIZE) {
            //createGcsSplitTask(bucketName, gcsObjectName, entityKey, l);
            createGcsSplitMessage(bucketName, gcsObjectName, entityKey, l);
            /*
            try {
            } catch (Exception e) {
              LOG.log(Level.WARNING, "Failed at pushing to " + DataModuleConstants.INGEST_QUEUE
                  + " :" + e.getMessage());
            }
            */
          }
        }
        LOG.log(Level.INFO, "done splitting " + gcsObjectName);
      }
    }

  }

  private void createGcsSplitTask(String bucketName, String gcsObjectName, String entityKey,
      long l) {
    Queue queue = QueueFactory.getQueue(DataModuleConstants.INGEST_QUEUE);
    queue.add(TaskOptions.Builder
        .withUrl(DataModuleConstants.GCSINGEST_URL)
        //.header(
        //    "Host",
        //    BackendServiceFactory.getBackendService()
        //        .getBackendAddress("ingestbackend"))
        .param(DataModuleConstants.INPUT_REQUEST_PARAM_BUCKET, bucketName)
        .param(DataModuleConstants.INGEST_REQUEST_PARAM_GCSOBJ, gcsObjectName)
        .param(DataModuleConstants.INPUT_REQUEST_PARAM_ENTITYKEY, entityKey)
        .param(DataModuleConstants.INGEST_REQUEST_PARAM_STARTINDEX,
            String.valueOf(l)));
  }
  
  private void createGcsSplitMessage(String bucketName, String gcsObjectName, String entityKey, 
      long l) throws IOException {
    Pubsub publisher = GcpUtil.createPubsubClient();
    /*
    Topic topic = new Topic().setName("motorola.com:datasystems/bluelake");
    try {
      publisher.topics().create(topic).execute();
    }
    catch (IOException e) {
      LOG.log(Level.WARNING, e.getMessage(), e);
    }
    */
    
    // Create a Pub/Sub message
    PubsubMessage pubsubMessage = new PubsubMessage();

    // It is recommended to send the payload Base64-encoded unless you know it will 
    // be safe to encode over JSON.
    String message = "Hello Cloud Pub/Sub!";
    pubsubMessage.encodeData(message.getBytes());

    // Create a label and add it to a list of labels to be included in the message
    java.util.List<Label> labels = new ArrayList<Label>(1);
    labels.add(new Label().setKey("bucket").setStrValue(bucketName));
    labels.add(new Label().setKey("object").setStrValue(gcsObjectName));
    labels.add(new Label().setKey("entityKey").setStrValue(entityKey));
    labels.add(new Label().setKey("start").setIntValue(l));
    pubsubMessage.setLabel(labels);
    
    // Publish the message to a topic
    PublishRequest publishRequest = new PublishRequest();
    publishRequest.setTopic("motorola.com:datasystems/bluelake").setMessage(pubsubMessage);

    publisher.topics().publish(publishRequest).execute();
    LOG.log(Level.INFO, "published input request message for start position " + l);
  }
}
