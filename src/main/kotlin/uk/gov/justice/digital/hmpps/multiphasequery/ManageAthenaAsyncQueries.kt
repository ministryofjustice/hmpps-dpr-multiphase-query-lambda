package uk.gov.justice.digital.hmpps.multiphasequery

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.LambdaLogger
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.logging.LogLevel
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.athena.AthenaClient
import software.amazon.awssdk.services.athena.model.QueryExecutionContext
import software.amazon.awssdk.services.athena.model.StartQueryExecutionRequest
import software.amazon.awssdk.services.redshiftdata.RedshiftDataClient
import software.amazon.awssdk.services.redshiftdata.model.*


class ManageAthenaAsyncQueries : RequestHandler<MutableMap<String, Any>, String> {
    private val redshiftClient: RedshiftDataClient = RedshiftDataClient.builder()
        .region(Region.EU_WEST_2)
        .build()


    override fun handleRequest(payload: MutableMap<String, Any>, context: Context?): String {
      if (context != null) {
          val logger = context.logger
          logger.log("Athena Query Management Lambda Invoked.", LogLevel.INFO)
          logger.log("Received event $payload", LogLevel.INFO)
          val queryExecutionId = (payload["detail"] as Map<String,Any>)?.get("queryExecutionId") as String
          val currentState = (payload["detail"] as Map<String,Any>)?.get("currentState") as String?
          logger.log("Current state: $currentState", LogLevel.INFO)
          logger.log("Current executionId: $queryExecutionId", LogLevel.INFO)
          if (queryExecutionId == null || currentState == null) {
              logger.log("No execution ID or current state found in the Event.", LogLevel.ERROR)
              throw RuntimeException("No execution ID or current state found in the Event.")
          }
          val updateStateQuery = "UPDATE datamart.admin.execution_manager SET current_state = '$currentState' WHERE current_execution_id = '$queryExecutionId'"
          queryRedshift(updateStateQuery, logger)
          if (currentState == "SUCCEEDED") {
              val nextQueryToRun = """
                 SELECT t2.query, t2.database, t2.catalog, t2.datasource, t2.root_execution_id, t2.index FROM
                  (SELECT root_execution_id, query, index FROM datamart.admin.execution_manager WHERE current_execution_id = '$queryExecutionId') AS t1
                  JOIN
                  (SELECT root_execution_id, index, query, database, catalog, datasource  FROM datamart.admin.execution_manager) AS t2
                 ON t1.root_execution_id = t2.root_execution_id AND t2.index = (t1.index + 1)
                  """
              logger.log("Running admin query", LogLevel.INFO)
              val getStatementResultResponse = queryRedshiftAndGetResult(nextQueryToRun, logger)
              val rootExecutionId = getData("root_execution_id", 0, getStatementResultResponse)
              val index = getIntData("index", 0, getStatementResultResponse)
              if (getStatementResultResponse.totalNumRows() == 1L) {
                  val query = getData("query", 0, getStatementResultResponse)
                  logger.log("Retrieved ${getStatementResultResponse.records()} results from admin table.", LogLevel.INFO)
                  val datasource = getData("datasource", 0, getStatementResultResponse)
                  if (datasource == "redshift") {
                      val redshiftStmExecutionId = queryRedshift(query, logger)
                      logger.log("Redshift statement execution ID: $redshiftStmExecutionId", LogLevel.INFO)
                      val updateStateQuery = "UPDATE datamart.admin.execution_manager SET current_state = 'SUCCEEDED', current_execution_id = '$redshiftStmExecutionId' WHERE root_execution_id = '$rootExecutionId' AND index = $index"
                      logger.log("Update state Redshift query: $updateStateQuery", LogLevel.INFO)
                      queryRedshift(updateStateQuery, logger)
                      return redshiftStmExecutionId
                  } else {
                      val database = getData("database", 0, getStatementResultResponse)
                      logger.log("The database from the admin table is: $database", LogLevel.INFO)
                      val catalog = getData("catalog", 0, getStatementResultResponse)
                      logger.log("The catalog from the admin table is: $catalog", LogLevel.INFO)
                      val athenaExecutionId = queryAthena(query, database, catalog, logger)
                      val updateStateQuery = "UPDATE datamart.admin.execution_manager SET current_execution_id = '$athenaExecutionId' WHERE root_execution_id = '$rootExecutionId' AND index = $index"
                      queryRedshift(updateStateQuery, logger)
                      return athenaExecutionId
                  }
              }
              logger.log("All queries succeeded. No further queries to run for rootExecutionId: $rootExecutionId.")
          } else if (currentState == "FAILED" || currentState == "CANCELLED") {
            val error = ((payload["detail"] as Map<String,Any>)["athenaError"] as Map<String,Any>)["errorMessage"] as String
            logger.log("Query with execution ID: $queryExecutionId failed. Error: $error",LogLevel.ERROR)
            val updateStateQuery = "UPDATE datamart.admin.execution_manager SET current_state = '$currentState', error = '$error'  WHERE current_execution_id = '$queryExecutionId'"
            queryRedshift(updateStateQuery, logger)
          }
    }
    return "Context was null."
  }

    private fun queryRedshift(query:String, logger: LambdaLogger): String {
        val statementRequest = ExecuteStatementRequest.builder()
            .clusterIdentifier("dpr-redshift-development")
            .database("datamart")
            .secretArn("arn:aws:secretsmanager:eu-west-2:771283872747:secret:dpr-redshift-secret-development-rLHcQZ")
            .sql(query)
            .build()
//        parameters?.let {
//            val idParam = SqlParameter.builder()
//                .name(it)
//                .value(it[])
//                .build()
//        }
        val executionId = redshiftClient.executeStatement(statementRequest).id()
        logger.log("Executed admin table statement and got ID: $executionId", LogLevel.DEBUG)
        val describeStatementRequest = DescribeStatementRequest.builder()
            .id(executionId)
            .build()
        var describeStatementResponse: DescribeStatementResponse
        do {
            Thread.sleep(500)
            describeStatementResponse = redshiftClient.describeStatement(describeStatementRequest)
            if (describeStatementResponse.status() == StatusString.FAILED) {
                logger.log("Statement with execution ID: $executionId failed with the following error: ${describeStatementResponse.error()}",
                    LogLevel.ERROR)
                throw RuntimeException("Statement with execution ID: $executionId failed.")
            } else if (describeStatementResponse.status() == StatusString.ABORTED) {
                logger.log("Statement with execution ID: $executionId was aborted", LogLevel.ERROR)
                throw RuntimeException("Statement with execution ID: $executionId was aborted.")
            }
        }
        while (describeStatementResponse.status() != StatusString.FINISHED)
        return executionId
    }

    private fun getRedshiftStatementResult(executionId: String): GetStatementResultResponse {
        val getStatementResultRequest = GetStatementResultRequest.builder()
            .id(executionId)
            .build()
        return redshiftClient.getStatementResult(getStatementResultRequest)
    }

    private fun queryRedshiftAndGetResult(query: String, logger: LambdaLogger): GetStatementResultResponse {
        return getRedshiftStatementResult(queryRedshift(query, logger))
    }

    private fun getData(columnName: String, rowNumber: Int, getStatementResultResponse: GetStatementResultResponse): String {
        val columnNameToResultIndex = mutableMapOf<String, Int>()
        getStatementResultResponse.columnMetadata().forEachIndexed{ i, colMetaData -> columnNameToResultIndex[colMetaData.name()] = i}
        return getStatementResultResponse.records()[rowNumber][columnNameToResultIndex[columnName]!!].stringValue()
    }

    private fun getIntData(columnName: String, rowNumber: Int, getStatementResultResponse: GetStatementResultResponse): Int {
        val columnNameToResultIndex = mutableMapOf<String, Int>()
        getStatementResultResponse.columnMetadata().forEachIndexed{ i, colMetaData -> columnNameToResultIndex[colMetaData.name()] = i}
        return getStatementResultResponse.records()[rowNumber][columnNameToResultIndex[columnName]!!].longValue().toInt()
    }

    private fun queryAthena(query:String, database: String, catalog: String, logger: LambdaLogger): String {
        val athenaClient: AthenaClient = AthenaClient.builder()
                  .region(Region.EU_WEST_2)
                  .build()
//              val database = "DIGITAL_PRISON_REPORTING"
//              val catalog = "nomis"
//              val query = "SELECT agy_loc_id FROM OMS_OWNER.LIVING_UNITS limit 10;"

              val queryExecutionContext = QueryExecutionContext.builder()
                  .database(database)
                  .catalog(catalog)
                  .build()
        /*
        """
                CREATE TABLE AwsDataCatalog.reports.testingeventbridge
                WITH (
                  format = 'PARQUET'
                )
                AS (
                SELECT * FROM TABLE(system.query(query =>

                )))
      """.trimIndent()
      */
          val startQueryExecutionRequest = StartQueryExecutionRequest.builder()
              .queryString(query)
              .queryExecutionContext(queryExecutionContext)
              .workGroup("dpr-generic-athena-workgroup")
              .build()
          logger.log("Full Athena async query: $query", LogLevel.INFO)
          val queryExecutionId = athenaClient
              .startQueryExecution(startQueryExecutionRequest).queryExecutionId()
          logger.log("Athena Query execution ID: $queryExecutionId", LogLevel.INFO)
        return queryExecutionId
    }
}