package uk.gov.justice.digital.hmpps.multiphasequery

import com.amazonaws.services.lambda.runtime.LambdaLogger
import com.amazonaws.services.lambda.runtime.logging.LogLevel
import uk.gov.justice.digital.hmpps.multiphasequery.data.AthenaRepository
import uk.gov.justice.digital.hmpps.multiphasequery.data.RedshiftRepository

class MultiphaseQueryService(
    private val athenaRepository: AthenaRepository,
    private val redshiftRepository: RedshiftRepository
) {

    fun updateStateAndMaybeExecuteNext(currentState: String, queryExecutionId: String, sequenceNumber: Int, logger: LambdaLogger, error: String? = null): String? {
        when (currentState) {
            "SUCCEEDED" -> {
                redshiftRepository.updateStateOfExistingExecution(currentState, sequenceNumber, queryExecutionId, logger)
                val nextQueryToRun = redshiftRepository.findNextQueryToExecute(queryExecutionId, logger)
                nextQueryToRun?.let {
                    val athenaExecutionId = athenaRepository.executeQuery(it.nextQueryToRun, it.database, it.catalog, logger)
                    redshiftRepository.updateWithNewExecutionId(athenaExecutionId, it.rootExecutionId, it.index, logger)
                    return athenaExecutionId
                }
                logger.log("All queries succeeded. No further queries to run.")
            }
            "FAILED" -> {
//                    val error = ((payload["detail"] as Map<String,Any>)["athenaError"] as Map<String,Any>)["errorMessage"] as String
                logger.log("Query with execution ID: $queryExecutionId failed. Error: $error", LogLevel.ERROR)
                redshiftRepository.updateStateOfExistingExecution(currentState, sequenceNumber, queryExecutionId, logger, error)
                return null
            }
            else -> {
                redshiftRepository.updateStateOfExistingExecution(currentState, sequenceNumber, queryExecutionId, logger)
                return null
            }
        }
        return null
    }
}