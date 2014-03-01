package com.bluelake.datamodule;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.JobReference;

public class Jobs {
  private static final Logger LOG = Logger.getLogger(BqService.class.getName());
  public static final String ID = "id";
  public static final String QUERY = "query";
  public static final String GCSFILES = "files";
  public static final String KIND = "kind";
  public static final String RESOURCE_JOB = "bluelake#job";
  private static Bigquery bigquery = GcpUtil.createBQClient();

  /**
   * Insert a new job, if successful, returns the id for the job
   * 
   * @param jsonObject
   * @return
   */
  public static JSONObject insert(JSONObject jsonObject) {
    if (jsonObject.has(QUERY)) {
      try {
        String querySql = jsonObject.getString(QUERY);
        JobReference jobRef = BqService.startQuery(bigquery, GcpUtil.getPROJECT_ID(), querySql);
        GcpUtil.createPollJobTask(GcpUtil.getPROJECT_ID(), jobRef.getJobId(),
            System.currentTimeMillis());
        return jobInsertedStatus();
      } catch (JSONException e) {
        LOG.log(Level.SEVERE, e.getMessage(), e);
      } catch (IOException e) {
        LOG.log(Level.SEVERE, e.getMessage(), e);
      }
    }
    else if (jsonObject.has(GCSFILES)) {
      
    }
    return null;
  }
  
  private static JSONObject jobInsertedStatus() {
    JSONObject status = new JSONObject();
    try {
      status.put(ID, "FIX THIS");
    } catch (JSONException e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
    }
    return status;
  }

}
