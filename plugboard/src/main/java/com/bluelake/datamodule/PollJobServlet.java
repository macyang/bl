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

    try {
      String jobObjS = req.getParameter(DataModuleConstants.JOBOBJECT);
      JSONObject jobObj = new JSONObject(jobObjS);
      JSONObject bqObj = jobObj.getJSONObject(Jobs.FIELD_BQ);
      String projectId = bqObj.getString(Jobs.BQ_PROJECTID);
      String jobId = bqObj.getString(Jobs.BQ_JOBID);
      int splitSize = bqObj.getInt(Jobs.BQ_SPLIT);

      Job pollJob = bigquery.jobs().get(projectId, jobId).execute();
      if (pollJob.getStatus().getState().equals("DONE")) {
        // split result
        GetQueryResultsResponse queryResult =
            bigquery.jobs().getQueryResults(projectId, jobId).execute();
        long totalRows = queryResult.getTotalRows().longValue();
        LOG.log(Level.INFO, "total rows: " + totalRows);
        for (long l = 0; l < totalRows; l += splitSize) {
          LOG.log(Level.INFO, "create split " + l + ":" + splitSize);
          GcpUtil.createBQSplitTask(jobObj, "", l);
        }
        resp.setStatus(HttpServletResponse.SC_OK);
      } else {
        // fail the request, retry by task queue
        LOG.log(Level.INFO, "BQ job not done, check again later, jobId: " + jobId);
        resp.setStatus(HttpServletResponse.SC_NOT_FOUND);
      }
    } catch (JSONException je) {
      LOG.log(Level.SEVERE, je.getMessage(), je);
    }
  }

}
