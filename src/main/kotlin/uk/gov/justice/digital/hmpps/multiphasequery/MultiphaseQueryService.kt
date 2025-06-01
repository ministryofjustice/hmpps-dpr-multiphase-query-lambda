package uk.gov.justice.digital.hmpps.multiphasequery

import com.amazonaws.services.lambda.runtime.LambdaLogger
import com.amazonaws.services.lambda.runtime.logging.LogLevel
import uk.gov.justice.digital.hmpps.multiphasequery.data.AthenaRepository
import uk.gov.justice.digital.hmpps.multiphasequery.data.RedshiftRepository
import uk.gov.justice.digital.hmpps.multiphasequery.data.RedshiftRepository.ResultRowNum

const val SUCCEEDED = "SUCCEEDED"
const val FAILED = "FAILED"
const val RETRY_TIMES = "RETRY_TIMES"
const val RETRY_DELAY = "RETRY_DELAY"

class MultiphaseQueryService(
    private val athenaRepository: AthenaRepository,
    private val redshiftRepository: RedshiftRepository,
    private val env: Env,
) {

    fun updateStateAndMaybeExecuteNext(currentState: String, queryExecutionId: String, sequenceNumber: Int, logger: LambdaLogger, error: String? = null): Long {
        when (currentState) {
            SUCCEEDED -> {
                var resultingRows = retry (logger) {
                    redshiftRepository.updateStateOfExistingExecution(currentState, sequenceNumber, queryExecutionId, logger)
                }
                val nextQueryToRun = redshiftRepository.findNextQueryToExecute(queryExecutionId, logger)
                nextQueryToRun?.let {
                    val athenaExecutionId = athenaRepository.executeQuery(it.nextQueryToRun, it.database, it.catalog, logger)
                    resultingRows += redshiftRepository.updateWithNewExecutionId(athenaExecutionId, it.rootExecutionId, it.index, logger)
                } ?: logger.log("All queries succeeded. No further queries to run.")
                return resultingRows
            }
            FAILED -> {
                logger.log("Query with execution ID: $queryExecutionId failed. Error: $error", LogLevel.ERROR)
                return retry (logger) { redshiftRepository.updateStateOfExistingExecution(currentState, sequenceNumber, queryExecutionId, logger, error) }
            }
            else -> {
                return retry (logger) { redshiftRepository.updateStateOfExistingExecution(currentState, sequenceNumber, queryExecutionId, logger) }
            }
        }
    }

    private fun retry(
        logger: LambdaLogger,
        times: Int = env.get(RETRY_TIMES)?.toInt() ?: 2,
        delayInMillis: Long = env.get(RETRY_DELAY)?.toLong()?: 500L,
        updateFun: () -> ResultRowNum): Long {
        var attempt = 0
        var resultingRows = updateFun()
        while (resultingRows.resultingRows == 0L && attempt < times ) {
            logger.log("No rows found to update for execution ID: ${resultingRows.executionId}. Retrying ${attempt + 1}", LogLevel.DEBUG)
            attempt++
            Thread.sleep(delayInMillis)
            resultingRows = updateFun()
        }
        return resultingRows.resultingRows
    }
}