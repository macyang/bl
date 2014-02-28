package com.bluelake.datamodule;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;
import com.google.api.services.bigquery.model.Job;

public class PollJobServlet extends HttpServlet {
  static final long serialVersionUID = 1234567890l;
  private static final Logger LOG = Logger.getLogger(PollJobServlet.class.getName());
  Bigquery bigquery = GcpUtil.createBQClient();

  /**
   * Waiting for the BQ job to be done. And once it's done, create split tasks.
   */
  public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    String projectId = req.getParameter(DataModuleConstants.INGEST_REQUEST_PARAM_PROJECTID);
    String jobId = req.getParameter(DataModuleConstants.INGEST_REQUEST_PARAM_JOBID);

    Job pollJob = bigquery.jobs().get(projectId, jobId).execute();
    if (pollJob.getStatus().getState().equals("DONE")) {
      // split result
      GetQueryResultsResponse queryResult =
          bigquery.jobs().getQueryResults(projectId, jobId).execute();
      long totalRows = queryResult.getTotalRows().longValue();
      LOG.log(Level.INFO, "total rows: " + totalRows);
      for (long l = 0; l < totalRows; l += DataModuleConstants.GCS_SPLITSIZE) {
        LOG.log(Level.INFO, "create split " + l + ":" + DataModuleConstants.GCS_SPLITSIZE);
        GcpUtil.createBQSplitTask(projectId, jobId, "", l);
      }
      resp.setStatus(HttpServletResponse.SC_OK);
    } else {
      // fail the request, retry by task queue
      LOG.log(Level.INFO, "BQ job not done, jobId: " + jobId);
      resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
    }
  }

}
