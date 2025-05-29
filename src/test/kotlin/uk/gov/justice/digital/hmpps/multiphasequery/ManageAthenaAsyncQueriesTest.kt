package uk.gov.justice.digital.hmpps.multiphasequery

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.LambdaLogger
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.kotlin.any
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import software.amazon.awssdk.services.athena.AthenaClient
import uk.gov.justice.digital.hmpps.multiphasequery.data.AthenaRepository
import uk.gov.justice.digital.hmpps.multiphasequery.data.RedshiftRepository

class ManageAthenaAsyncQueriesTest {

    private val multiphaseQueryService: MultiphaseQueryService = mock()
    private val context: Context = mock()
    private val logger: LambdaLogger = mock()

    @BeforeEach
    fun setUp() {
        whenever(context.logger).thenReturn(logger)
    }

    @Test
    fun `handleRequest returns the execution ID of the next query to execute when there is one`() {
        val manageAthenaAsyncQueries = ManageAthenaAsyncQueries(multiphaseQueryService)
        val queryExecutionId = "queryExecutionId"
        val currentState = "RUNNING"
        val sequenceNumber = 1
        val nextQueryExecutionId = "nextQueryExecutionId"
        val payload: MutableMap<String, Any> = mutableMapOf(
            "detail"  to mutableMapOf(
                "queryExecutionId" to queryExecutionId,
                "currentState" to currentState,
                "sequenceNumber" to "$sequenceNumber"
            ),
        )
        manageAthenaAsyncQueries.handleRequest(payload, context)
        whenever(multiphaseQueryService.updateStateAndMaybeExecuteNext(any(), any(), any(), any(), any())).thenReturn(nextQueryExecutionId)
        verify(multiphaseQueryService, times(1)).updateStateAndMaybeExecuteNext(currentState, queryExecutionId, sequenceNumber, logger)
    }
}