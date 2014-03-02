package com.bluelake.datahub.udf;

import java.util.List;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;

/*
 * Process a single BigQuery table row.
 */
public interface TableRowUdf {

  public void processTableRow(List<TableFieldSchema> fieldSchema, TableRow row);
}
