package uk.gov.justice.digital.hmpps.multiphasequery

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.logging.LogLevel
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.athena.AthenaClient
import software.amazon.awssdk.services.redshiftdata.RedshiftDataClient
import uk.gov.justice.digital.hmpps.multiphasequery.data.AthenaRepository
import uk.gov.justice.digital.hmpps.multiphasequery.data.RedshiftRepository


class ManageAthenaAsyncQueries(
    private val multiphaseQueryService: MultiphaseQueryService = MultiphaseQueryService(
        AthenaRepository(
            AthenaClient.builder()
                .region(Region.EU_WEST_2)
                .build()),
        RedshiftRepository(
            RedshiftDataClient.builder()
                .region(Region.EU_WEST_2)
                .build()),
    )
) : RequestHandler<MutableMap<String, Any>, String> {

    override fun handleRequest(payload: MutableMap<String, Any>, context: Context?): String {
        if (context != null) {
            val logger = context.logger
            logger.log("Athena Query Management Lambda Invoked.", LogLevel.INFO)
            logger.log("Received event $payload", LogLevel.INFO)
            val queryExecutionId = extract(payload, "queryExecutionId")
            val currentState = extract(payload, "currentState")
            val sequenceNumber = extract(payload, "sequenceNumber").toInt()
            val error = extractError(payload)
            logger.log("Current state: $currentState", LogLevel.DEBUG)
            logger.log("Current executionId: $queryExecutionId", LogLevel.DEBUG)
            if (queryExecutionId == null || currentState == null) {
                logger.log("No execution ID or current state found in the Event.", LogLevel.ERROR)
                throw RuntimeException("No execution ID or current state found in the Event.")
            }
            multiphaseQueryService.updateStateAndMaybeExecuteNext(currentState, queryExecutionId, sequenceNumber, logger, error)?.let { return it }
        }
        return "Context was null."
    }

    private fun extract(payload: MutableMap<String, Any>, key: String): String {
        return (payload["detail"] as Map<String,Any>)?.get(key) as String
    }

    private fun extractError(payload: MutableMap<String, Any>): String? {
        return ((payload["detail"] as Map<String,Any>)["athenaError"] as Map<String,Any>?)?.get("errorMessage") as String?
    }

}