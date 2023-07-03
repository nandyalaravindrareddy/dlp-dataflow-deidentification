package com.google.swarm.tokenization.common;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableResult;
import com.google.swarm.tokenization.DLPTextToBigQueryStreamingV2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BigQueryOps {
    public static final Logger LOG = LoggerFactory.getLogger(BigQueryOps.class);
    public static void updateRecord(String datasetName,String tableName,String status,String fileName){
        String dmlQuery = String.format(
                "UPDATE %s.%s SET status = '%s' WHERE file_name='%s'",
                datasetName, tableName,status, fileName);
        try {
            BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
            LOG.info("dmlQuery: {}", dmlQuery);
            QueryJobConfiguration dmlQueryConfig =
                    QueryJobConfiguration.newBuilder(dmlQuery)
                            .build();
            TableResult result = bigQuery.query(dmlQueryConfig);
            if(result.getTotalRows()==0) throw new RuntimeException("Failed to update status in file registration table");
            result.iterateAll().forEach(rows -> rows.forEach(row -> System.out.println(row.getValue())));
        }catch(Exception e){
            LOG.error("failed to update dmlQuery {} due to {}",dmlQuery,e.getMessage());
            throw new RuntimeException("Failed to update status in file registration table");
        }
    }
}
