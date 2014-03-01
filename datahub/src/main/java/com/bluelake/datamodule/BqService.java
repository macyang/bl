package com.bluelake.datamodule;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;

public class BqService {
  static final long serialVersionUID = 1234567890l;
  private static final Logger LOG = Logger.getLogger(BqService.class.getName());
  
  private BqService() {}

  /**
   * Inserts a Query Job for a particular query
   */
  public static JobReference insertJob(JSONObject jobObj)
      throws JSONException, IOException {
    
    LOG.log(Level.INFO, "Inserting Query Job: " + jobObj.toString());
    JSONObject bqObj = jobObj.getJSONObject(Jobs.FIELD_BQ);
    String querySql = bqObj.getString(Jobs.BQ_QUERY);

    Job job = new Job();
    JobConfiguration config = new JobConfiguration();
    JobConfigurationQuery queryConfig = new JobConfigurationQuery();
    config.setQuery(queryConfig);

    job.setConfiguration(config);
    queryConfig.setQuery(querySql);

    Bigquery bigquery = GcpUtil.createBQClient();
    Insert insert = bigquery.jobs().insert(GcpUtil.getPROJECT_ID(), job);
    insert.setProjectId(GcpUtil.getPROJECT_ID());
    JobReference jobRef = insert.execute().getJobReference();
    
    bqObj.put(Jobs.BQ_PROJECTID, jobRef.getProjectId());
    bqObj.put(Jobs.BQ_JOBID, jobRef.getJobId());
    GcpUtil.createPollJobTask(jobObj, System.currentTimeMillis());

    LOG.log(Level.INFO, "Job ID of Query Job is: " + jobRef.getJobId());

    return jobRef;
  }

}
