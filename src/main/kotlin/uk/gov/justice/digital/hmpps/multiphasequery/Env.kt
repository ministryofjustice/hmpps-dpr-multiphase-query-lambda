package uk.gov.justice.digital.hmpps.multiphasequery

open class Env {
    open fun get(name: String): String? = System.getenv(name)
}