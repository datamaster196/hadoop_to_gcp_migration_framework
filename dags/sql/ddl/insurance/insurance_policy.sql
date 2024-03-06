CREATE TABLE IF NOT EXISTS `{{ var.value.INTEGRATION_PROJECT }}.insurance.insurance_policy`(
    insurance_policy_renewals_adw_key    STRING    NOT NULL,
    insurance_policy_adw_key             STRING    NOT NULL,
    insurance_agent_adw_key              STRING    NOT NULL,
    insurance_agency_adw_key             STRING    NOT NULL,
    insurance_client_adw_key             STRING,
    insurance_coverage_adw_key           STRING    NOT NULL,
    insurance_driver_adw_key             STRING,
    insurance_vehicle_adw_key            STRING    NOT NULL
)
;