package com.bluelake.datahub.udf;

import com.google.api.services.bigquery.model.GetQueryResultsResponse;

/*
 * Process a single BigQuery table split task.
 */
public interface TableSplitUdf {

  public void processTableSplit(GetQueryResultsResponse queryResult);
}
