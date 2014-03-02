package com.bluelake.datahub;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Arrays;

import javax.servlet.http.HttpServletRequest;

import org.json.JSONException;
import org.json.JSONObject;

import com.google.api.client.extensions.appengine.http.UrlFetchTransport;
import com.google.api.client.googleapis.extensions.appengine.auth.oauth2.AppIdentityCredential;
import com.google.api.client.googleapis.services.json.CommonGoogleJsonClientRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson.JacksonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.PubsubScopes;
import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions;

public class GcpUtil {
  // Suggested format for application names is MyCompany-MyProject/Version
  private static final String APPLICATION_NAME = "moto-bluelake/0.1";
  private static final HttpTransport TRANSPORT = new UrlFetchTransport();
  private static final JsonFactory JSON_FACTORY = new JacksonFactory();
  private static final String BIGQUERY_SCOPE = "https://www.googleapis.com/auth/bigquery";
  // Obtain this from https://cloud.google.com/console/project/your_project/apiui/credential
  private static final String API_KEY = "AIzaSyBTWoCMTJbGaD3lzGgUAbAYptu-9pmMGlU";

  private static String PROJECT_ID = "motorola.com:datasystems";

  public static String getPROJECT_ID() {
    return PROJECT_ID;
  }

  public static void setPROJECT_ID(String pROJECT_ID) {
    PROJECT_ID = pROJECT_ID;
  }

  private GcpUtil() {}

  public static Pubsub createPubsubClient() {
    HttpRequestInitializer credential = new AppIdentityCredential(PubsubScopes.all());

    Pubsub client =
        new Pubsub.Builder(TRANSPORT, JSON_FACTORY, credential)
            .setApplicationName(APPLICATION_NAME)
            .setGoogleClientRequestInitializer(
                new CommonGoogleJsonClientRequestInitializer(API_KEY)).build();

    return client;
  }

  public static Bigquery createBQClient() {
    HttpRequestInitializer credential =
        new AppIdentityCredential(Arrays.asList(new String[] {BIGQUERY_SCOPE}));

    Bigquery bigquery =
        new Bigquery.Builder(TRANSPORT, JSON_FACTORY, credential).setApplicationName(
            APPLICATION_NAME).build();

    return bigquery;
  }

  public static void createPollJobTask(JSONObject jobObj, long starttime) {
    Queue queue = QueueFactory.getQueue(DataHubConstants.BQ_QUEUE);
    queue.add(TaskOptions.Builder
        .withUrl(DataHubConstants.POLLJOB_URL)
        // .header(
        // "Host",
        // BackendServiceFactory.getBackendService()
        // .getBackendAddress("ingestbackend"))
        .param(DataHubConstants.JOBOBJECT, jobObj.toString())
        .param(DataHubConstants.INGEST_REQUEST_PARAM_STARTTIME, String.valueOf(starttime)));
  }

  public static void createBQSplitTask(JSONObject jobObj, String entityKey,
      long startIndex) {
    Queue queue = QueueFactory.getQueue(DataHubConstants.INGEST_QUEUE);
    queue.add(TaskOptions.Builder
        .withUrl(DataHubConstants.BQINGEST_URL)
        // .header(
        // "Host",
        // BackendServiceFactory.getBackendService()
        // .getBackendAddress("ingestbackend"))
        .param(DataHubConstants.JOBOBJECT, jobObj.toString())
        .param(DataHubConstants.INPUT_REQUEST_PARAM_ENTITYKEY, entityKey)
        .param(DataHubConstants.INGEST_REQUEST_PARAM_STARTINDEX, String.valueOf(startIndex)));
  }

  public static JSONObject getJSONRequest(HttpServletRequest request) throws IOException {
    StringBuffer jb = new StringBuffer();
    String line = null;
    try {
      BufferedReader reader = request.getReader();
      while ((line = reader.readLine()) != null)
        jb.append(line);
    } catch (Exception e) {
      throw new IOException("Error reading JSON request string");
    }

    try {
      JSONObject jsonObject = new JSONObject(jb.toString());
      return jsonObject;
    } catch (JSONException e) {
      throw new IOException("Error parsing JSON request string");
    }
  }
}
