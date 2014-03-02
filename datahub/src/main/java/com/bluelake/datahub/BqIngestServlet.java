package com.bluelake.datahub;

import java.io.IOException;
import java.math.BigInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONException;
import org.json.JSONObject;

import com.bluelake.datahub.udf.DefaultTableSplitUdf;
import com.bluelake.datahub.udf.TableSplitUdf;
import com.bluelake.datahub.udf.UdfFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.GetQueryResultsResponse;

public class BqIngestServlet extends HttpServlet {
  static final long serialVersionUID = 1234567890l;
  private static final Logger LOG = Logger.getLogger(BqIngestServlet.class.getName());

  public void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {

    try {
      String jobObjS = req.getParameter(DataHubConstants.JOBOBJECT);
      JSONObject jobObj = new JSONObject(jobObjS);
      JSONObject bqObj = jobObj.getJSONObject(Jobs.FIELD_BQ);
      String projectId = bqObj.getString(Jobs.BQ_PROJECTID);
      String jobId = bqObj.getString(Jobs.BQ_JOBID);
      long splitSize = bqObj.getLong(Jobs.BQ_SPLIT);


      String startIndexS = req.getParameter(DataHubConstants.INGEST_REQUEST_PARAM_STARTINDEX);
      if (startIndexS != null) {
        LOG.log(Level.INFO, "startIndex: " + startIndexS);
        LOG.log(Level.INFO, "splitSize: " + splitSize);
        
        Bigquery bigquery = GcpUtil.createBQClient();
        GetQueryResultsResponse queryResult =
            bigquery.jobs().getQueryResults(projectId, jobId)
                .setStartIndex(new BigInteger(startIndexS))
                .setMaxResults(splitSize).execute();
        
        TableSplitUdf tableSplitUdf = null;
        String tableSplitUdfName = null;
        if (bqObj.has(Jobs.BQ_TABLESPLITUDF)) {
          tableSplitUdfName = bqObj.getString(Jobs.BQ_TABLESPLITUDF);
        }
        if (tableSplitUdfName != null) {
          tableSplitUdf = UdfFactory.getTableSplitUdf(tableSplitUdfName);
        }
        if (tableSplitUdf == null) {
          tableSplitUdf = new DefaultTableSplitUdf();
        }
        tableSplitUdf.init(jobObj);
        tableSplitUdf.processTableSplit(queryResult);
        
        resp.setStatus(HttpServletResponse.SC_OK);
      }
    } catch (JSONException je) {
      LOG.log(Level.SEVERE, je.getMessage(), je);
    }
  }

}
