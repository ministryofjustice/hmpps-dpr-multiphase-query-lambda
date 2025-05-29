package uk.gov.justice.digital.hmpps.multiphasequery.data

import com.amazonaws.services.lambda.runtime.LambdaLogger
import com.amazonaws.services.lambda.runtime.logging.LogLevel
import software.amazon.awssdk.services.redshiftdata.RedshiftDataClient
import software.amazon.awssdk.services.redshiftdata.model.*
import java.time.Instant
import java.util.Base64

class RedshiftRepository(private val redshiftClient: RedshiftDataClient) {

    fun findNextQueryToExecute(queryExecutionId: String, logger: LambdaLogger): NextQuery? {
        val queryToFindNextQueryToRun = """
                 SELECT t2.query, t2.database, t2.catalog, t2.datasource, t2.root_execution_id, t2.index FROM
                  (SELECT root_execution_id, query, index FROM datamart.admin.execution_manager WHERE current_execution_id = '$queryExecutionId') AS t1
                  JOIN
                  (SELECT root_execution_id, index, query, database, catalog, datasource  FROM datamart.admin.execution_manager) AS t2
                 ON t1.root_execution_id = t2.root_execution_id AND t2.index = (t1.index + 1)
                  """
        logger.log("Running admin query to find next query to run", LogLevel.DEBUG)
        val getStatementResultResponse = queryAndGetResult(queryToFindNextQueryToRun, logger)
        logger.log("Retrieved ${getStatementResultResponse.totalNumRows()} results from admin table.", LogLevel.DEBUG)
        return if (getStatementResultResponse.totalNumRows() == 1L) {
            logger.log("There is a next query to run.", LogLevel.DEBUG)
            val nextQuery = NextQuery(
                catalog = getData("catalog", 0, getStatementResultResponse),
                database = getData("database", 0, getStatementResultResponse),
                datasource = getData("datasource", 0, getStatementResultResponse),
                index = getIntData("index", 0, getStatementResultResponse),
                rootExecutionId = getData("root_execution_id", 0, getStatementResultResponse),
                nextQueryToRun = String(Base64.getDecoder().decode(getData("query", 0, getStatementResultResponse).removeSurrounding("\"").toByteArray()))
            )
            logger.log("Next Query is: $nextQuery", LogLevel.DEBUG)
            return nextQuery
        } else {
            null
        }
    }

    fun updateStateOfExistingExecution(
        currentState: String,
        sequenceNumber: Int,
        queryExecutionId: String,
        logger: LambdaLogger,
        error: String? = null
    ) {
        val maybeError = error?.let{"error = '${Base64.getEncoder().encodeToString(error.toByteArray())}',"} ?: ""
        val updateStateQuery = "UPDATE datamart.admin.execution_manager SET current_state = '$currentState', $maybeError sequence_number = $sequenceNumber, last_update = '${Instant.now()}' WHERE current_execution_id = '$queryExecutionId' AND sequence_number < $sequenceNumber"
        executeQueryAndWaitForCompletion(updateStateQuery, logger)
    }

    fun updateWithNewExecutionId(athenaExecutionId: String, sequenceNumber: Int, rootExecutionId: String, index: Int, logger: LambdaLogger) {
        val updateStateQuery = "UPDATE datamart.admin.execution_manager SET current_execution_id = '$athenaExecutionId', sequence_number = $sequenceNumber, last_update = '${Instant.now()}' WHERE root_execution_id = '${rootExecutionId}' AND index = $index"
        executeQueryAndWaitForCompletion(updateStateQuery, logger)
    }

    private fun queryAndGetResult(query: String, logger: LambdaLogger): GetStatementResultResponse {
        return getStatementResult(executeQueryAndWaitForCompletion(query, logger))
    }

    private fun executeQueryAndWaitForCompletion(query:String, logger: LambdaLogger): String {
        val statementRequest = ExecuteStatementRequest.builder()
            .clusterIdentifier(System.getenv("CLUSTER_ID"))
            .database(System.getenv("DB_NAME"))
            .secretArn(System.getenv("CREDENTIAL_SECRET_ARN"))
            .sql(query)
            .build()
//        parameters?.let {
//            val idParam = SqlParameter.builder()
//                .name(it)
//                .value(it[])
//                .build()
//        }
        val executionId = redshiftClient.executeStatement(statementRequest).id()
        logger.log("Executing admin table query: $query", LogLevel.DEBUG)
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
        logger.log("Finished executing statement with id: $executionId, status: ${describeStatementResponse.statusAsString()}, result rows: ${describeStatementResponse.resultRows()}", LogLevel.DEBUG)
        return executionId
    }

    private fun getStatementResult(executionId: String): GetStatementResultResponse {
        val getStatementResultRequest = GetStatementResultRequest.builder()
            .id(executionId)
            .build()
        return redshiftClient.getStatementResult(getStatementResultRequest)
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
}