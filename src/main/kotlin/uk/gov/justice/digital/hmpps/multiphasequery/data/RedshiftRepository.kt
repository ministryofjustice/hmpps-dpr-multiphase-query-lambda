package uk.gov.justice.digital.hmpps.multiphasequery.data

import com.amazonaws.services.lambda.runtime.LambdaLogger
import com.amazonaws.services.lambda.runtime.logging.LogLevel
import software.amazon.awssdk.services.redshiftdata.RedshiftDataClient
import software.amazon.awssdk.services.redshiftdata.model.*
import uk.gov.justice.digital.hmpps.multiphasequery.Env
import java.time.Instant
import java.util.Base64

const val REDSHIFT_STATUS_POLLING_WAIT_MS = "REDSHIFT_STATUS_POLLING_WAIT_MS"

const val REDSHIFT_CLUSTER_ID = "CLUSTER_ID"
const val REDSHIFT_DB_NAME = "DB_NAME"
const val REDSHIFT_CREDENTIAL_SECRET_ARN = "CREDENTIAL_SECRET_ARN"

class RedshiftRepository(private val redshiftClient: RedshiftDataClient, private val env: Env) {

    fun findNextQueryToExecute(queryExecutionId: String, logger: LambdaLogger): NextQuery? {
        val queryToFindNextQueryToRun = """
                 SELECT t2.query, t2.database, t2.catalog, t2.datasource_name, t2.root_execution_id, t2.index FROM
                  (SELECT root_execution_id, query, index FROM datamart.admin.multiphase_query_state WHERE current_execution_id = '$queryExecutionId') AS t1
                  JOIN
                  (SELECT root_execution_id, index, query, database, catalog, datasource_name  FROM datamart.admin.multiphase_query_state) AS t2
                 ON t1.root_execution_id = t2.root_execution_id AND t2.index = (t1.index + 1)
                  """
        logger.log("Running admin query to find next query to run", LogLevel.DEBUG)
        val getStatementResultResponse = executeQueryAndGetResult(queryToFindNextQueryToRun, logger)
        logger.log("Retrieved ${getStatementResultResponse.totalNumRows()} results from admin table.", LogLevel.DEBUG)
        return if (getStatementResultResponse.totalNumRows() == 1L) {
            logger.log("There is a next query to run.", LogLevel.DEBUG)
            val nextQuery = NextQuery(
                catalog = getData("catalog", 0, getStatementResultResponse),
                database = getData("database", 0, getStatementResultResponse),
                datasourceName = getData("datasource_name", 0, getStatementResultResponse),
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
    ): ResultRowNum {
        val maybeError = error?.let{"error = '${Base64.getEncoder().encodeToString(error.toByteArray())}',"} ?: ""
        //Update the current state only if the sequence number of the Athena State Change event is greater than the existing sequence number stored in Redshift. i.e. only if this is a next event in the sequence.
        val updateStateQuery = "UPDATE datamart.admin.multiphase_query_state SET current_state = '$currentState', $maybeError sequence_number = $sequenceNumber, last_update = SYSDATE WHERE current_execution_id = '$queryExecutionId' AND sequence_number < $sequenceNumber"
        return executeQueryAndWaitForCompletion(updateStateQuery, logger)
    }

    fun updateWithNewExecutionId(athenaExecutionId: String, rootExecutionId: String, index: Int, logger: LambdaLogger): Long {
        val updateStateQuery = "UPDATE datamart.admin.multiphase_query_state SET current_execution_id = '$athenaExecutionId', last_update = SYSDATE WHERE root_execution_id = '${rootExecutionId}' AND index = $index"
        return executeQueryAndWaitForCompletion(updateStateQuery, logger).resultingRows
    }

    private fun executeQueryAndGetResult(query: String, logger: LambdaLogger): GetStatementResultResponse {
        return getStatementResult(executeQueryAndWaitForCompletion(query, logger).executionId)
    }

    private fun executeQueryAndWaitForCompletion(query:String, logger: LambdaLogger): ResultRowNum {
        val statementRequest = ExecuteStatementRequest.builder()
            .clusterIdentifier(env.get(REDSHIFT_CLUSTER_ID))
            .database(env.get(REDSHIFT_DB_NAME))
            .secretArn(env.get(REDSHIFT_CREDENTIAL_SECRET_ARN))
            .sql(query)
            .build()
//        parameters?.let {
//            val idParam = SqlParameter.builder()
//                .name(it)
//                .value(it[])
//                .build()
//        }
        logger.log("Executing admin table query: $query", LogLevel.DEBUG)
        val executionId = redshiftClient.executeStatement(statementRequest).id()
        logger.log("Executed admin table statement and got ID: $executionId", LogLevel.DEBUG)
        val describeStatementRequest = DescribeStatementRequest.builder()
            .id(executionId)
            .build()
        var describeStatementResponse: DescribeStatementResponse
        do {
            Thread.sleep(env.get(REDSHIFT_STATUS_POLLING_WAIT_MS)?.toLong() ?: 500)
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
        return ResultRowNum(executionId, describeStatementResponse.resultRows())
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

    data class ResultRowNum(val executionId: String, val resultingRows: Long)
}