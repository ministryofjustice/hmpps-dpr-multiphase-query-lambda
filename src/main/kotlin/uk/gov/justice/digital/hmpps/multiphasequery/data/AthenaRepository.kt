package uk.gov.justice.digital.hmpps.multiphasequery.data

import com.amazonaws.services.lambda.runtime.LambdaLogger
import com.amazonaws.services.lambda.runtime.logging.LogLevel
import software.amazon.awssdk.services.athena.AthenaClient
import software.amazon.awssdk.services.athena.model.QueryExecutionContext
import software.amazon.awssdk.services.athena.model.StartQueryExecutionRequest

class AthenaRepository(private val athenaClient: AthenaClient) {

    fun executeQuery(query:String, database: String, catalog: String, logger: LambdaLogger): String {
        val queryExecutionContext = QueryExecutionContext.builder()
            .database(database)
            .catalog(catalog)
            .build()
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