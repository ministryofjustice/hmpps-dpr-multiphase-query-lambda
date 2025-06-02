package uk.gov.justice.digital.hmpps.multiphasequery

import uk.gov.justice.digital.hmpps.multiphasequery.data.REDSHIFT_CLUSTER_ID
import uk.gov.justice.digital.hmpps.multiphasequery.data.REDSHIFT_CREDENTIAL_SECRET_ARN
import uk.gov.justice.digital.hmpps.multiphasequery.data.REDSHIFT_DB_NAME
import uk.gov.justice.digital.hmpps.multiphasequery.data.REDSHIFT_STATUS_POLLING_WAIT_MS

const val REDSHIFT_CLUSTER_ID_VALUE = "REDSHIFT_CLUSTER_ID"
const val REDSHIFT_DB_NAME_VALUE = "REDSHIFT_DB_NAME"
const val REDSHIFT_CREDENTIAL_SECRET_ARN_VALUE = "REDSHIFT_CREDENTIAL_SECRET_ARN"

class FakeEnv(
    private val properties: Map<String, String> = mapOf(
        RETRY_TIMES to "2",
        RETRY_DELAY to "0",
        REDSHIFT_STATUS_POLLING_WAIT_MS to "0",
        REDSHIFT_CLUSTER_ID to REDSHIFT_CLUSTER_ID_VALUE,
        REDSHIFT_DB_NAME to REDSHIFT_DB_NAME_VALUE,
        REDSHIFT_CREDENTIAL_SECRET_ARN to REDSHIFT_CREDENTIAL_SECRET_ARN_VALUE,
    )
): Env() {

    override fun get(name: String): String? {
        return properties[name]
    }
}