package com.bluelake.datamodule;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONException;
import org.json.JSONObject;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;

public class BqService extends HttpServlet {
  static final long serialVersionUID = 1234567890l;
  private static final Logger LOG = Logger.getLogger(BqService.class.getName());

  public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {

        //"SELECT imei, devicetime, event FROM [motorola.com:datasystems:mac.bt_discharge_summary] LIMIT 30";
        // "SELECT imei, ltime FROM [motorola.com:science-cluster:dev_devicestats.201402010000] LIMIT 30";
    resp.setStatus(HttpServletResponse.SC_OK);
  }

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
