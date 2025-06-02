package uk.gov.justice.digital.hmpps.multiphasequery.data

import com.amazonaws.services.lambda.runtime.LambdaLogger
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.mock
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import software.amazon.awssdk.services.athena.AthenaClient
import software.amazon.awssdk.services.athena.model.QueryExecutionContext
import software.amazon.awssdk.services.athena.model.StartQueryExecutionRequest
import software.amazon.awssdk.services.athena.model.StartQueryExecutionResponse

class AthenaRepositoryTest {
    @Test
    fun `executeQuery should call the athenaClient startQueryExecution with the correct arguments and return the execution ID`() {
        val athenaClient = mock<AthenaClient>()
        val startQueryExecutionResponse = mock<StartQueryExecutionResponse>()
        val athenaRepository = AthenaRepository(athenaClient)
        val database = "testDb"
        val catalog = "testCatalog"
        val query = "SELECT * FROM a"
        val workgroup = "dpr-generic-athena-workgroup"
        val executionId = "executionId"
        val logger = mock<LambdaLogger>()
        val queryExecutionContext = QueryExecutionContext.builder()
            .database(database)
            .catalog(catalog)
            .build()
        val startQueryExecutionRequest = StartQueryExecutionRequest.builder()
            .queryString(
                query
            )
            .queryExecutionContext(queryExecutionContext)
            .workGroup(workgroup)
            .build()

        whenever(
            athenaClient.startQueryExecution(
                ArgumentMatchers.any(StartQueryExecutionRequest::class.java),
            ),
        ).thenReturn(startQueryExecutionResponse)
        whenever(
            startQueryExecutionResponse.queryExecutionId(),
        ).thenReturn(executionId)

        val actual = athenaRepository.executeQuery(query, database, catalog, logger)

        verify(athenaClient, times(1)).startQueryExecution(startQueryExecutionRequest)
        assertEquals(executionId, actual)
    }
}