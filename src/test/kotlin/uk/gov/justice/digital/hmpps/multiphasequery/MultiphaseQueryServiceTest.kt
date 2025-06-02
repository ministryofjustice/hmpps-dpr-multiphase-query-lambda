package uk.gov.justice.digital.hmpps.multiphasequery

import com.amazonaws.services.lambda.runtime.LambdaLogger
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.Mockito.mock
import org.mockito.kotlin.any
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import uk.gov.justice.digital.hmpps.multiphasequery.data.AthenaRepository
import uk.gov.justice.digital.hmpps.multiphasequery.data.NextQuery
import uk.gov.justice.digital.hmpps.multiphasequery.data.RedshiftRepository
import java.util.UUID

class MultiphaseQueryServiceTest {

    private val athenaRepository = mock<AthenaRepository>()
    private val redshiftRepository = mock<RedshiftRepository>()
    private val logger: LambdaLogger = mock()
    private val multiphaseQueryService = MultiphaseQueryService(athenaRepository, redshiftRepository, FakeEnv())

    @Test
    fun `when the state is succeeded and there is no next query only the state of the current query gets updated`() {
        val queryExecutionId = UUID.randomUUID().toString()
        val sequenceNumber = 1
        val resultRowNum = RedshiftRepository.ResultRowNum("someId", 1)

        whenever(redshiftRepository.updateStateOfExistingExecution(any(), any(), any(), any(), anyOrNull())).thenReturn(resultRowNum)

        val actual = multiphaseQueryService.updateStateAndMaybeExecuteNext(SUCCEEDED, queryExecutionId, sequenceNumber, logger)

        verify(redshiftRepository, times(1)).updateStateOfExistingExecution(SUCCEEDED, sequenceNumber, queryExecutionId, logger, null)
        verify(redshiftRepository, times(1)).findNextQueryToExecute(queryExecutionId, logger)
        verify(redshiftRepository, times(0)).updateWithNewExecutionId(any(), any(), any(), any())
        assertEquals(1, actual)
    }

    @Test
    fun `when the state is succeeded and there is a next query, the state of the current query gets updated and the next query gets updated with its new execution ID`() {
        val queryExecutionId = UUID.randomUUID().toString()
        val sequenceNumber = 1
        val resultRowNum = RedshiftRepository.ResultRowNum("someId", 1)
        val catalog = "catalog"
        val database = "db"
        val datasource = "datasource"
        val rootExecutionId = "rootId"
        val nextQueryToRun = "SELECT * FROM a"
        val nexQueryIndex = 1
        val nextQuery = NextQuery(catalog, database, datasource, nexQueryIndex, rootExecutionId, nextQueryToRun)
        val executionIdOfNextQuery = UUID.randomUUID().toString()

        whenever(redshiftRepository.updateStateOfExistingExecution(any(), any(), any(), any(), anyOrNull())).thenReturn(resultRowNum)
        whenever(redshiftRepository.findNextQueryToExecute(any(), any())).thenReturn(nextQuery)
        whenever(athenaRepository.executeQuery(any(), any(), any(), any())).thenReturn(executionIdOfNextQuery)
        whenever(redshiftRepository.updateWithNewExecutionId(any(), any(), any(), any())).thenReturn(1)

        val actual = multiphaseQueryService.updateStateAndMaybeExecuteNext(SUCCEEDED, queryExecutionId, sequenceNumber, logger)

        verify(redshiftRepository, times(1)).updateStateOfExistingExecution(SUCCEEDED, sequenceNumber, queryExecutionId, logger, null)
        verify(redshiftRepository, times(1)).findNextQueryToExecute(queryExecutionId, logger)
        verify(athenaRepository, times(1)).executeQuery(nextQueryToRun, database, catalog, logger)
        verify(redshiftRepository, times(1)).updateWithNewExecutionId(executionIdOfNextQuery, rootExecutionId, nexQueryIndex, logger)
        assertEquals(2, actual)
    }

    @ParameterizedTest
    @ValueSource(strings = [FAILED, "QUEUED", SUCCEEDED, "RUNNING", "CANCELLED"])
    fun `updateStateOfExistingExecution should be called for any state`(currentState: String) {
        val queryExecutionId = UUID.randomUUID().toString()
        val sequenceNumber = 1
        val resultRowNum = RedshiftRepository.ResultRowNum("someId", 1)
        var maybeError: String? = null

        whenever(redshiftRepository.updateStateOfExistingExecution(any(), any(), any(), any(), anyOrNull())).thenReturn(resultRowNum)
        if (currentState == FAILED) {
            maybeError = "Error"
        }

        val actual = multiphaseQueryService.updateStateAndMaybeExecuteNext(currentState, queryExecutionId, sequenceNumber, logger, maybeError)

        verify(redshiftRepository, times(1)).updateStateOfExistingExecution(currentState, sequenceNumber, queryExecutionId, logger, maybeError)
        assertEquals(1, actual)
    }

    @ParameterizedTest
    @ValueSource(strings = [FAILED, "QUEUED", SUCCEEDED, "RUNNING", "CANCELLED"])
    fun `updateStateOfExistingExecution should be retried up to 2 times for any state until it updates the state`(currentState: String) {
        val queryExecutionId = UUID.randomUUID().toString()
        val sequenceNumber = 1
        val resultRowNum = RedshiftRepository.ResultRowNum("someId", 0)
        var maybeError: String? = null

        whenever(redshiftRepository.updateStateOfExistingExecution(any(), any(), any(), any(), anyOrNull())).thenReturn(resultRowNum)
        if (currentState == FAILED) {
            maybeError = "Error"
        }

        val actual = multiphaseQueryService.updateStateAndMaybeExecuteNext(currentState, queryExecutionId, sequenceNumber, logger, maybeError)

        verify(redshiftRepository, times(3)).updateStateOfExistingExecution(currentState, sequenceNumber, queryExecutionId, logger, maybeError)
        assertEquals(0, actual)
    }

    @ParameterizedTest
    @ValueSource(strings = [FAILED, "QUEUED", SUCCEEDED, "RUNNING", "CANCELLED"])
    fun `updateStateOfExistingExecution should stop retrying when the state gets updated`(currentState: String) {
        val queryExecutionId = UUID.randomUUID().toString()
        val sequenceNumber = 1
        val resultRowNum1 = RedshiftRepository.ResultRowNum("someId", 0)
        val resultRowNum2 = RedshiftRepository.ResultRowNum("someId", 1)
        var maybeError: String? = null

        whenever(redshiftRepository.updateStateOfExistingExecution(any(), any(), any(), any(), anyOrNull())).thenReturn(resultRowNum1, resultRowNum2)
        if (currentState == FAILED) {
            maybeError = "Error"
        }

        val actual = multiphaseQueryService.updateStateAndMaybeExecuteNext(currentState, queryExecutionId, sequenceNumber, logger, maybeError)

        verify(redshiftRepository, times(2)).updateStateOfExistingExecution(currentState, sequenceNumber, queryExecutionId, logger, maybeError)
        assertEquals(1, actual)
    }
}
