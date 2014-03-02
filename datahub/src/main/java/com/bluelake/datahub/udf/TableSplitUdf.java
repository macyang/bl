package com.bluelake.datahub.udf;

import org.json.JSONObject;

import com.google.api.services.bigquery.model.GetQueryResultsResponse;

/*
 * Process a single BigQuery table split task. Call the init() first with the Job object so it has a
 * chance to grab and store any information needed out of the Job object.
 */
public interface TableSplitUdf {

  public void init(JSONObject jobObj);

  public void processTableSplit(GetQueryResultsResponse queryResult);
}
