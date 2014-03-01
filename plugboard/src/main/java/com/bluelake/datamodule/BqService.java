package com.bluelake.datamodule;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Jobs.Insert;
import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;

public class BqService extends HttpServlet {
  static final long serialVersionUID = 1234567890l;
  private static final Logger LOG = Logger.getLogger(BqService.class.getName());
  Bigquery bigquery = GcpUtil.createBQClient();

  public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {

    // Start a Query Job
    String querySql =
        "SELECT imei, devicetime, event FROM [motorola.com:datasystems:mac.bt_discharge_summary] LIMIT 30";
        // "SELECT imei, ltime FROM [motorola.com:science-cluster:dev_devicestats.201402010000] LIMIT 30";
    JobReference jobRef = startQuery(bigquery, GcpUtil.getPROJECT_ID(), querySql);
    GcpUtil.createPollJobTask(GcpUtil.getPROJECT_ID(), jobRef.getJobId(),
        System.currentTimeMillis());
    resp.setStatus(HttpServletResponse.SC_OK);
  }

  /**
   * Inserts a Query Job for a particular query
   */
  public static JobReference startQuery(Bigquery bigquery, String projectId, String querySql)
      throws IOException {
    LOG.log(Level.INFO, "Inserting Query Job: " + querySql);

    Job job = new Job();
    JobConfiguration config = new JobConfiguration();
    JobConfigurationQuery queryConfig = new JobConfigurationQuery();
    config.setQuery(queryConfig);

    job.setConfiguration(config);
    queryConfig.setQuery(querySql);

    Insert insert = bigquery.jobs().insert(projectId, job);
    insert.setProjectId(projectId);
    JobReference jobRef = insert.execute().getJobReference();

    LOG.log(Level.INFO, "Job ID of Query Job is: " + jobRef.getJobId());

    return jobRef;
  }

}
