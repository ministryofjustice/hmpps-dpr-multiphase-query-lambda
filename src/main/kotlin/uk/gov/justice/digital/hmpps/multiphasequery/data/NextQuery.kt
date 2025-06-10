package uk.gov.justice.digital.hmpps.multiphasequery.data

data class NextQuery(
    val catalog: String,
    val database: String,
    val datasourceName: String? = null,
    val index: Int,
    val rootExecutionId: String,
    val nextQueryToRun: String,
) {
}