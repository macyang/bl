package com.bluelake.datahub;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

public class Jobs {

  private static final Logger LOG = Logger.getLogger(BqService.class.getName());

  public static final String FIELD_KIND = "kind";
  public static final String RESOURCE_JOB = "bluelake#job";
  public static final String FIELD_ID = "id";

  public static final String FIELD_BQ = "bq";
  public static final String BQ_QUERY = "query";
  public static final String BQ_SPLIT = "split";
  public static final String BQ_PROJECTID = "projectId";
  public static final String BQ_JOBID = "jobId";

  public static final String FIELD_STATUS = "status";
  public static final String STATUS_STATE = "state";

  public static final String FIELD_GCS = "gcs";


  /**
   * Insert a new job, if successful, returns the id for the job
   * 
   * @param jobObj
   * @return
   */
  public static JSONObject insert(JSONObject jobObj) {

    if (jobObj.has(FIELD_BQ)) {
      try {
        BqService.insertJob(jobObj);
        return jobStatus("TODO: return job id here");
      } catch (JSONException e) {
        LOG.log(Level.SEVERE, e.getMessage(), e);
      } catch (IOException e) {
        LOG.log(Level.SEVERE, e.getMessage(), e);
      }
    } else if (jobObj.has(FIELD_GCS)) {

    }
    return null;
  }

  private static JSONObject jobStatus(String jobId) {

    JSONObject ret = new JSONObject();
    JSONObject status = new JSONObject();

    try {
      status.put(STATUS_STATE, "running");
      ret.put(FIELD_ID, jobId);
      ret.put(FIELD_STATUS, status);
    } catch (JSONException e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
    }
    return ret;
  }

}
