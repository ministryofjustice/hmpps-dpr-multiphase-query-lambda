package uk.gov.justice.digital.hmpps.multiphasequery

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.LambdaLogger
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.kotlin.any
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.mockito.kotlin.anyOrNull

class ManageAthenaAsyncQueriesTest {

    private val multiphaseQueryService: MultiphaseQueryService = mock()
    private val context: Context = mock()
    private val logger: LambdaLogger = mock()
    private val manageAthenaAsyncQueries = ManageAthenaAsyncQueries(FakeEnv(), multiphaseQueryService)

    @BeforeEach
    fun setUp() {
        whenever(context.logger).thenReturn(logger)
    }

    @Test
    fun `handleRequest returns the number of rows updated`() {
        val queryExecutionId = "queryExecutionId"
        val currentState = "SUCCEEDED"
        val sequenceNumber = 1
        val payload: MutableMap<String, Any> = mutableMapOf(
            "detail"  to mutableMapOf(
                "queryExecutionId" to queryExecutionId,
                "currentState" to currentState,
                "sequenceNumber" to "$sequenceNumber"
            ),
        )
        whenever(multiphaseQueryService.updateStateAndMaybeExecuteNext(any(), any(), any(), any(), anyOrNull())).thenReturn(1)
        val actual = manageAthenaAsyncQueries.handleRequest(payload, context)
        verify(multiphaseQueryService, times(1)).updateStateAndMaybeExecuteNext(currentState, queryExecutionId, sequenceNumber, logger)
        assertEquals("Completed execution and updated 1 rows.", actual)
    }

    @Test
    fun `handleRequest returns context was null if the Lambda Context was null`() {
        val queryExecutionId = "queryExecutionId"
        val currentState = "SUCCEEDED"
        val sequenceNumber = 1
        val payload: MutableMap<String, Any> = mutableMapOf(
            "detail"  to mutableMapOf(
                "queryExecutionId" to queryExecutionId,
                "currentState" to currentState,
                "sequenceNumber" to "$sequenceNumber"
            ),
        )
        val actual = manageAthenaAsyncQueries.handleRequest(payload, null)
        verify(multiphaseQueryService, times(0)).updateStateAndMaybeExecuteNext(any(), any(), any(), any(), anyOrNull())
        assertEquals("Context was null.", actual)
    }
}