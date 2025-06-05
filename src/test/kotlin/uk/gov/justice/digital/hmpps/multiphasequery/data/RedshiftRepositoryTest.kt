package uk.gov.justice.digital.hmpps.multiphasequery.data

import com.amazonaws.services.lambda.runtime.LambdaLogger
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.ArgumentMatchers
import org.mockito.Mockito
import org.mockito.Mockito.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import software.amazon.awssdk.services.redshiftdata.RedshiftDataClient
import software.amazon.awssdk.services.redshiftdata.model.*
import uk.gov.justice.digital.hmpps.multiphasequery.*
import uk.gov.justice.digital.hmpps.multiphasequery.data.RedshiftRepository.*
import java.time.Instant
import java.util.UUID

class RedshiftRepositoryTest {

    private val redshiftDataClient = mock<RedshiftDataClient>()
    private val redshiftRepository = RedshiftRepository(redshiftDataClient, FakeEnv())
    private val logger = mock<LambdaLogger>()
    private val queryExecutionId = "executionId"
    private val catalog = "someCatalog"
    private val database = "db"
    private val datasourceName = "datasourceName"
    private val rootExecutionId = "someRootExecutionId"
    private val index = 1L
    private val selectStatementExecutionId = "selectStatementExecutionId"
    private val executeStatementResponse = mock<ExecuteStatementResponse>()
    private val describeStatementResponse = mock<DescribeStatementResponse>()

    @Test
    fun `findNextQueryToExecute returns the next query if it exists`() {
        val executeStatementRequest = setUpExecuteStatementRequest(findNextQuerySelectSql())
        val describeStatementRequest = setUpDescribeStatementRequest(selectStatementExecutionId)
        val getStatementResultRequest = setUpGetStatementResultRequest(selectStatementExecutionId)
        val getStatementResultResponse: GetStatementResultResponse = GetStatementResultResponse
            .builder()
            .columnMetadata(buildHeaderRow())
            .records(buildSingleRowResult())
            .totalNumRows(1)
            .build()

        whenever(redshiftDataClient.executeStatement(ArgumentMatchers.any(ExecuteStatementRequest::class.java),),).thenReturn(executeStatementResponse)
        whenever(executeStatementResponse.id()).thenReturn(selectStatementExecutionId)
        whenever(redshiftDataClient.describeStatement(ArgumentMatchers.any(DescribeStatementRequest::class.java))).thenReturn(describeStatementResponse)
        whenever(describeStatementResponse.status()).thenReturn(StatusString.FINISHED)
        whenever(redshiftDataClient.getStatementResult(ArgumentMatchers.any(GetStatementResultRequest::class.java))).thenReturn(getStatementResultResponse)

        val actual = redshiftRepository.findNextQueryToExecute(queryExecutionId, logger)

        verify(redshiftDataClient, times(1)).executeStatement(executeStatementRequest)
        verify(redshiftDataClient, times(1)).describeStatement(describeStatementRequest)
        verify(redshiftDataClient, times(1)).getStatementResult(getStatementResultRequest)
        assertEquals(NextQuery(catalog, database, datasourceName, index.toInt(), rootExecutionId, "SELECT * FROM a"), actual)
    }

    @Test
    fun `findNextQueryToExecute returns null if there is no next query`() {
        val executeStatementRequest = setUpExecuteStatementRequest(findNextQuerySelectSql())
        val describeStatementRequest = setUpDescribeStatementRequest(selectStatementExecutionId)
        val getStatementResultRequest = setUpGetStatementResultRequest(selectStatementExecutionId)
        val getStatementResultResponse: GetStatementResultResponse = GetStatementResultResponse
            .builder()
            .columnMetadata(buildHeaderRow())
            .records(listOf(emptyList()))
            .totalNumRows(0)
            .build()

        whenever(redshiftDataClient.executeStatement(ArgumentMatchers.any(ExecuteStatementRequest::class.java),),).thenReturn(executeStatementResponse)
        whenever(executeStatementResponse.id()).thenReturn(selectStatementExecutionId)
        whenever(redshiftDataClient.describeStatement(ArgumentMatchers.any(DescribeStatementRequest::class.java))).thenReturn(describeStatementResponse)
        whenever(describeStatementResponse.status()).thenReturn(StatusString.FINISHED)
        whenever(redshiftDataClient.getStatementResult(ArgumentMatchers.any(GetStatementResultRequest::class.java))).thenReturn(getStatementResultResponse)

        val actual = redshiftRepository.findNextQueryToExecute(queryExecutionId, logger)

        verify(redshiftDataClient, times(1)).executeStatement(executeStatementRequest)
        verify(redshiftDataClient, times(1)).describeStatement(describeStatementRequest)
        verify(redshiftDataClient, times(1)).getStatementResult(getStatementResultRequest)
        assertNull(actual)
    }

    @ParameterizedTest
    @ValueSource(strings = [SUCCEEDED, FAILED])
    fun `updateStateOfExistingExecution updates the state and returns the execution ID of the update and the rows updated`(state: String) {
        val updateExecutionId = UUID.randomUUID().toString()
        val sequenceNumber = 3
        val maybeError: String? = (if (state == FAILED) "Something went wrong." else null)
        val executeStatementRequest = setUpExecuteStatementRequest(updateStateOfExistingExecutionSql(state, sequenceNumber, maybeError?.let { "U29tZXRoaW5nIHdlbnQgd3Jvbmcu" }))
        val describeStatementRequest = setUpDescribeStatementRequest(updateExecutionId)

        whenever(redshiftDataClient.executeStatement(ArgumentMatchers.any(ExecuteStatementRequest::class.java),),).thenReturn(executeStatementResponse)
        whenever(executeStatementResponse.id()).thenReturn(updateExecutionId)
        whenever(redshiftDataClient.describeStatement(ArgumentMatchers.any(DescribeStatementRequest::class.java))).thenReturn(describeStatementResponse)
        whenever(describeStatementResponse.status()).thenReturn(StatusString.FINISHED)
        whenever(describeStatementResponse.resultRows()).thenReturn(1L)

        val actual = redshiftRepository.updateStateOfExistingExecution(state, sequenceNumber, queryExecutionId, logger, maybeError)

        verify(redshiftDataClient, times(1)).executeStatement(executeStatementRequest)
        verify(redshiftDataClient, times(1)).describeStatement(describeStatementRequest)
        assertEquals(ResultRowNum(updateExecutionId,1), actual)
    }

    @Test
    fun `updateWithNewExecutionId updates the matching rootExecution row with the new executionId for that index`() {
        val updateExecutionId = UUID.randomUUID().toString()
        val executeStatementRequest = setUpExecuteStatementRequest(updateWithNewExecutionIdSql())
        val describeStatementRequest = setUpDescribeStatementRequest(updateExecutionId)
        val updatedRows = 1L

        whenever(redshiftDataClient.executeStatement(ArgumentMatchers.any(ExecuteStatementRequest::class.java),),).thenReturn(executeStatementResponse)
        whenever(executeStatementResponse.id()).thenReturn(updateExecutionId)
        whenever(redshiftDataClient.describeStatement(ArgumentMatchers.any(DescribeStatementRequest::class.java))).thenReturn(describeStatementResponse)
        whenever(describeStatementResponse.status()).thenReturn(StatusString.FINISHED)
        whenever(describeStatementResponse.resultRows()).thenReturn(updatedRows)

        val actual = redshiftRepository.updateWithNewExecutionId(queryExecutionId, rootExecutionId, index.toInt(), logger)

        verify(redshiftDataClient, times(1)).executeStatement(executeStatementRequest)
        verify(redshiftDataClient, times(1)).describeStatement(describeStatementRequest)
        assertEquals(updatedRows, actual)
    }

    private fun buildSingleRowResult() =
        listOf(
            buildRow(listOf(database, catalog, datasourceName, rootExecutionId, "\"U0VMRUNUICogRlJPTSBh\"")).plus(
                buildIndexField(
                    index
                )
            )
        )

    private fun setUpGetStatementResultRequest(selectStatementExecutionId: String): GetStatementResultRequest? =
        GetStatementResultRequest.builder()
            .id(selectStatementExecutionId)
            .build()

    private fun setUpDescribeStatementRequest(statementExecutionId: String): DescribeStatementRequest? =
        DescribeStatementRequest.builder()
            .id(statementExecutionId)
            .build()

    private fun setUpExecuteStatementRequest(query: String): ExecuteStatementRequest? =
        ExecuteStatementRequest.builder()
            .clusterIdentifier(REDSHIFT_CLUSTER_ID_VALUE)
            .database(REDSHIFT_DB_NAME_VALUE)
            .secretArn(REDSHIFT_CREDENTIAL_SECRET_ARN_VALUE)
            .sql(query)
            .build()

    private fun findNextQuerySelectSql(): String {
        return """
                 SELECT t2.query, t2.database, t2.catalog, t2.datasource, t2.root_execution_id, t2.index FROM
                  (SELECT root_execution_id, query, index FROM datamart.admin.execution_manager WHERE current_execution_id = '$queryExecutionId') AS t1
                  JOIN
                  (SELECT root_execution_id, index, query, database, catalog, datasource  FROM datamart.admin.execution_manager) AS t2
                 ON t1.root_execution_id = t2.root_execution_id AND t2.index = (t1.index + 1)
                  """
    }

    private fun updateStateOfExistingExecutionSql(state: String, sequenceNumber: Int, maybeError: String?): String {
        return "UPDATE datamart.admin.execution_manager SET current_state = '$state', ${maybeError?.let { "error = '$it'," } ?: ""} sequence_number = $sequenceNumber, last_update = SYSDATE WHERE current_execution_id = '$queryExecutionId' AND sequence_number < $sequenceNumber"
    }

    private fun updateWithNewExecutionIdSql(): String {
        return "UPDATE datamart.admin.execution_manager SET current_execution_id = '$queryExecutionId', last_update = SYSDATE WHERE root_execution_id = '${rootExecutionId}' AND index = $index"
    }

    private fun buildHeaderRow(): List<ColumnMetadata>  {
        return listOf("database", "catalog", "datasource", "root_execution_id", "query", "index").map { ColumnMetadata.builder().name(it).build() }
    }

    private fun buildRow(columns: List<String>): List<Field> {
        return columns.map { Field.builder().stringValue(it).build() }
    }

    private fun buildIndexField(index: Long): Field? = Field.builder().longValue(index).build()
}