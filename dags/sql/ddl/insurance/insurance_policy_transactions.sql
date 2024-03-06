CREATE TABLE IF NOT EXISTS `{{ var.value.INTEGRATION_PROJECT }}.insurance.insurance_policy_transactions`(
    insurance_policy_transactions_adw_key    STRING    NOT NULL,
    insurance_policy_renewals_adw_key        STRING    NOT NULL,
    insurance_client_adw_key                 STRING    NOT NULL,
    insurance_agent_adw_key                  STRING    NOT NULL,
    insurance_agency_adw_key                 STRING    NOT NULL
)
;