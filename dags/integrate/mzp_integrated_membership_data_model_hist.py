#######################################################################################################################
##  DAG_NAME :  integrated_membership_mzp_hist
##  PURPOSE :   Executes historical load for conatct info and membership (mzp) model
##  PREREQUISITE DAGs : ingest_mzp
##                      ingest_csaa
##                      ingest_eds
##                      ingest_sigma_add_assoc_file
##                      ingest_mzp_aca_store_list
#######################################################################################################################

from datetime import datetime, timedelta

from airflow import DAG
from airflow import models
from utils import ingestion_utilities as iu
from utils import integration_utilities as int_u
from airflow.models import Variable
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

SUCCESS_EMAIL = Variable.get("email_recipient_success", default_var='') or None
FAIL_EMAIL = Variable.get("email_recipient_failure", default_var='') or None


DAG_TITLE = "integrated_membership_mzp_hist"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 12, 19),
    'email': [FAIL_EMAIL],
    'email_on_success': True,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=360),
    'params': {
        'dag_name': DAG_TITLE
    }
}

with DAG(DAG_TITLE, default_args=default_args, schedule_interval=None,
         template_searchpath='/home/airflow/gcs/dags/sql/') as dag:
    ######################################################################
    # TASKS

    # Dimensions
    ## name
    task_name_insert = int_u.run_query('insert_dim_name', "dml/integration/incremental/dim_name/mzp/dim_name_mzp.sql")

    ## phone
    task_phone_insert = int_u.run_query('insert_dim_phone', "dml/integration/incremental/dim_phone/mzp/dim_phone_mzp.sql")

    ## address
    task_address_insert = int_u.run_query('insert_dim_address', "dml/integration/incremental/dim_address/mzp/dim_address_mzp.sql")

    ## email
    task_email_insert = int_u.run_query('insert_dim_email', "dml/integration/incremental/dim_email/mzp/dim_email_mzp.sql")

	
    # Contact Matching Logic 
    task_contact_work_source = int_u.run_query("build_contact_work_source","dml/integration/historical/contact_info/mzp/contact_mzp_work_source.sql")
    task_contact_work_target = int_u.run_query("build_contact_work_target", "dml/integration/incremental/contact_info/mzp/contact_mzp_work_target.sql")
    task_contact_work_stage = int_u.run_query("build_contact_work_matched", "dml/integration/incremental/contact_info/mzp/contact_mzp_work_matching.sql")


    # Contact XREF's
    ## name
    task_xref_contact_name_merge = int_u.run_query('merge_name', "dml/integration/incremental/contact_info/mzp/xref_contact_name_mzp_merge.sql")

    ## address
    task_xref_contact_address_merge = int_u.run_query('merge_address', "dml/integration/incremental/contact_info/mzp/xref_contact_address_mzp_merge.sql")

    ## phone
    task_xref_contact_phone_merge = int_u.run_query('merge_phone', "dml/integration/incremental/contact_info/mzp/xref_contact_phone_mzp_merge.sql")

    ## email
    task_xref_contact_email_merge = int_u.run_query('merge_email', "dml/integration/incremental/contact_info/mzp/xref_contact_email_mzp_merge.sql")

    ## keys
    task_dim_contact_source_key_merge = int_u.run_query('merge_keys', "dml/integration/incremental/contact_info/mzp/dim_contact_source_key_mzp_merge.sql")

    # contact
    task_contact_insert = int_u.run_query('insert_contact', "dml/integration/incremental/contact_info/mzp/dim_contact_info_mzp_insert.sql")
    task_contact_merge = int_u.run_query('merge_contact', "dml/integration/incremental/contact_info/shared/dim_contact_info_merge.sql")
    task_contact_match_audit = int_u.run_query('match_audit', "dml/integration/incremental/contact_info/mzp/audit_contact_match_mzp_insert.sql")

    # Product
    task_product_source = int_u.run_query('product_source',
                                          "dml/integration/historical/dim_product/mzp_product_work_source.sql")
    task_product_transformed = int_u.run_query('product_transformed',
                                               "dml/integration/historical/dim_product/mzp_product_work_transformed.sql")
    task_product_stage = int_u.run_query('product_stage',
                                         "dml/integration/historical/dim_product/mzp_product_work_type_2_hist.sql")
    task_product_merge = int_u.run_query('product_merge', "dml/integration/historical/dim_product/dim_product.sql")

    # Membership

    # Membership - Workarea loads dml/integration/historical/dim_membership/
    task_membership_work_source = int_u.run_query("build_membership_work_source",
                                                  "dml/integration/historical/dim_membership/mzp_membership_work_source.sql")
    task_membership_work_transformed = int_u.run_query("build_membership_work_transformed",
                                                       "dml/integration/historical/dim_membership/mzp_membership_work_transformed.sql")
    task_membership_work_final_staging = int_u.run_query("build_membership_work_final_staging",
                                                         "dml/integration/historical/dim_membership/mzp_membership_work_type_2_hist.sql")
    task_membership_merge = int_u.run_query("merge_membership",
                                            "dml/integration/historical/dim_membership/dim_membership.sql")

    # Member

    # Member - Workarea loads

    task_member_work_source = int_u.run_query("build_member_work_source",
                                              "dml/integration/historical/dim_member/mzp_member_work_source.sql")
    task_member_work_transformed = int_u.run_query("build_member_work_transformed",
                                                   "dml/integration/historical/dim_member/mzp_member_work_transformed.sql")
    task_member_work_final_staging = int_u.run_query("build_member_work_final_staging",
                                                     "dml/integration/historical/dim_member/mzp_member_work_type_2_hist.sql")
    task_member_merge = int_u.run_query('merge_member', "dml/integration/historical/dim_member/dim_member.sql")

    # Member_rider

    # Member_rider - Workarea loads

    task_member_rider_work_source = int_u.run_query("build_member_rider_work_source",
                                                    "dml/integration/historical/dim_member_rider/mzp_member_rider_work_source.sql")
    task_member_rider_work_transformed = int_u.run_query("build_member_rider_work_transformed",
                                                         "dml/integration/historical/dim_member_rider/mzp_member_rider_work_transformed.sql")
    task_member_rider_work_final_staging = int_u.run_query("build_member_rider_work_final_staging",
                                                           "dml/integration/historical/dim_member_rider/mzp_member_rider_work_type_2_hist.sql")
    task_member_rider_merge = int_u.run_query('merge_member_rider',
                                              "dml/integration/historical/dim_member_rider/dim_member_rider.sql")

    # Employee

    # Employee -Workarea Loads
    task_employee_work_source = int_u.run_query("build_employee_work_source",
                                                "dml/integration/historical/dim_employee/dim_employee_source.sql")
    task_employee_work_transformed = int_u.run_query("build_employee_work_transformed",
                                                     "dml/integration/historical/dim_employee/mzp_employee_work_transformed.sql")
    task_employee_work_final_staging = int_u.run_query("build_employee_work_final_staging",
                                                       "dml/integration/historical/dim_employee/employee_work_stage.sql")
    # Employee - Merge Load
    task_employee_merge = int_u.run_query('merge_employee', "dml/integration/historical/dim_employee/dim_employee.sql")

    # Employee_role
    # Employee_role - Workarea loads
    task_employee_role_work_source = int_u.run_query("build_employee_role_work_source",
                                                     "dml/integration/historical/dim_employee_role/mzp_employee_role_work_source.sql")
    task_employee_role_work_transformed = int_u.run_query("build_employee_role_work_transformed",
                                                          "dml/integration/historical/dim_employee_role/mzp_employee_role_work_transformed.sql")
    task_employee_role_work_final_staging = int_u.run_query("build_employee_role_work_final_staging",
                                                            "dml/integration/historical/dim_employee_role/mzp_employee_role_work_type_2_hist.sql")
    # member - Merge Load
    task_employee_role_merge = int_u.run_query('merge_employee_role',
                                               "dml/integration/historical/dim_employee_role/dim_employee_role.sql")

    # member_comments

    task_membership_comments_source = int_u.run_query('membership_comment_source', 'dml/integration/incremental/dim_comment/membership_comment_source.sql')
    task_membership_comment_fkey = int_u.run_query('membership_comment_fkey', 'dml/integration/incremental/dim_comment/membership_comment_fkey.sql')
    task_membership_comment_stage = int_u.run_query('membership_comment_stage', 'dml/integration/incremental/dim_comment/membership_comment_stage.sql')
    task_membership_comment = int_u.run_query('membership_comment', 'dml/integration/incremental/dim_comment/dim_comment.sql')


    # membership segment

    task_membership_segment_source = int_u.run_query('membership_segment_source',
                                                     "dml/integration/historical/dim_membership_marketing_segmentation/mzp_membership_segment_source.sql")
    task_membership_segment_transformed = int_u.run_query('membership_segment_transformed',
                                                          "dml/integration/historical/dim_membership_marketing_segmentation/mzp_membership_segment_transformed.sql")
    task_membership_segment_stage = int_u.run_query('membership_segment_stage',
                                                    "dml/integration/historical/dim_membership_marketing_segmentation/mzp_membership_segment_stage.sql")
    task_membership_segment_merge = int_u.run_query('membership_segment_merge',
                                                    "dml/integration/historical/dim_membership_marketing_segmentation/dim_membership_marketing_segmentation.sql")

    # member_auto_renewal_card_
    task_member_auto_renewal_card_work_source = int_u.run_query('member_auto_renewal_card_work_source',
                                                                "dml/integration/historical/dim_member_auto_renewal_card/mzp_auto_renewal_card_work_source.sql")
    task_member_auto_renewal_card_work_transformed = int_u.run_query('member_auto_renewal_card_work_transformed',
                                                                     "dml/integration/historical/dim_member_auto_renewal_card/mzp_auto_renewal_card_work_transformed.sql")
    task_member_auto_renewal_card_final_stage = int_u.run_query('member_auto_renewal_card_final_stage',
                                                                "dml/integration/historical/dim_member_auto_renewal_card/mzp_auto_renewal_card_work_type_2_hist.sql")
    task_merge_member_auto_renewal_card = int_u.run_query('merge_member_auto_renewal_card',
                                                          "dml/integration/historical/dim_member_auto_renewal_card/dim_member_auto_renewal_card.sql")

    # billing_summary

    # billing_summary - Workarea loads
    task_billing_summary_work_source = int_u.run_query("build_billing_summary_work_source",
                                                       "dml/integration/historical/membership_billing_summary/mzp_membership_billing_summary_work_source.sql")
    task_billing_summary_work_transformed = int_u.run_query("build_billing_summary_work_transformed",
                                                            "dml/integration/historical/membership_billing_summary/mzp_membership_billing_summary_work_transformed.sql")
    task_billing_summary_work_final_staging = int_u.run_query("build_billing_summary_work_final_staging",
                                                              "dml/integration/historical/membership_billing_summary/mzp_membership_billing_summary_work_type_2_hist.sql")

    # billing_summary - Merge Load
    task_billing_summary_merge = int_u.run_query("merge_billing_summary",
                                                 "dml/integration/historical/membership_billing_summary/membership_billing_summary.sql")

    # billing_detail

    # billing_detail - Workarea loads

    task_membership_billing_detail_work_source = int_u.run_query("build_membership_billing_detail_work_source",
                                                                 "dml/integration/historical/membership_billing_detail/mzp_membership_billing_detail_work_source.sql")
    task_membership_billing_detail_work_transformed = int_u.run_query(
        "build_membership_billing_detail_work_transformed",
        "dml/integration/historical/membership_billing_detail/mzp_membership_billing_detail_work_transformed.sql")
    task_billing_detail_work_final_staging = int_u.run_query("build_billing_detail_work_final_staging",
                                                             "dml/integration/historical/membership_billing_detail/mzp_membership_billing_detail_work_type_2_hist.sql")
    task_billing_detail_merge = int_u.run_query('merge_billing_detail',
                                                "dml/integration/historical/membership_billing_detail/membership_billing_detail.sql")

    # membership_office

    # membership_office - Workarea loads
    task_membership_office_work_source = int_u.run_query("build_membership_office_work_source",
                                                         "dml/integration/historical/dim_aca_office/mzp_aca_office_work_source.sql")
    task_membership_office_work_transformed = int_u.run_query("build_membership_office_work_transformed",
                                                              "dml/integration/historical/dim_aca_office/mzp_aca_office_work_transformed.sql")
    task_membership_office_work_final_staging = int_u.run_query("build_membership_office_work_final_staging",
                                                                "dml/integration/historical/dim_aca_office/mzp_aca_office_work_type_2_hist.sql")

    # membership_office - Merge Load
    task_membership_office_merge = int_u.run_query('merge_membership_office',
                                                   "dml/integration/historical/dim_aca_office/dim_aca_office.sql")

    # Payment Summary
    task_payment_applied_summary_source = int_u.run_query("payment_applied_summary_source",
                                                          "dml/integration/historical/membership_payment_applied_summary/mzp_membership_payment_applied_summary_work_source.sql")
    task_payment_applied_summary_transformed = int_u.run_query("payment_applied_summary_transformed",
                                                               "dml/integration/historical/membership_payment_applied_summary/mzp_membership_payment_applied_summary_work_transformed.sql")
    task_payment_summary_stage = int_u.run_query("payment_summary_stage",
                                                 "dml/integration/historical/membership_payment_applied_summary/mzp_membership_payment_applied_summary_work_type_2_hist.sql")
    task_payment_summary_merge = int_u.run_query('payment_summary_merge',
                                                 "dml/integration/historical/membership_payment_applied_summary/membership_payment_applied_summary.sql")
    task_payment_summary_reversal_update = int_u.run_query('payment_summary_reversal_update',
                                                           "dml/integration/historical/membership_payment_applied_summary/payment_applied_summary_reversal_update.sql")

    # membership_gl_payments_applied

    task_membership_gl_payments_applied_work_source = int_u.run_query("membership_gl_payments_applied_work_source",
                                                                      "dml/integration/historical/dim_membership_gl_payments_applied/mzp_gl_payments_work_source.sql")
    task_membership_gl_payments_applied_work_transformed = int_u.run_query(
        "membership_gl_payments_applied_work_transformed",
        "dml/integration/historical/dim_membership_gl_payments_applied/mzp_gl_payments_work_transformed.sql")
    task_membership_gl_payments_applied_work_final_staging = int_u.run_query(
        "membership_gl_payments_applied_work_final_staging",
        "dml/integration/historical/dim_membership_gl_payments_applied/mzp_gl_payments_work_type_2_hist.sql")
    task_membership_gl_payments_applied_merge = int_u.run_query("membership_gl_payments_applied_merge",
                                                                "dml/integration/historical/dim_membership_gl_payments_applied/dim_membership_gl_payments_applied.sql")

    # member_payments_received - Merge Load

    task_membership_payments_received_work_source = int_u.run_query("Membership_payments_received_work_source",
                                                                    "dml/integration/historical/membership_payments_received/mzp_membership_payments_received_work_source.sql")
    task_membership_payments_received_work_transformed = int_u.run_query(
        "Membership_payments_received_work_transformed",
        "dml/integration/historical/membership_payments_received/mzp_payments_received_work_transformed.sql")
    task_membership_payments_received_work_final_staging = int_u.run_query(
        "build_membership_payments_received_work_final_staging",
        "dml/integration/historical/membership_payments_received/mzp_payments_received_work_type_2_hist.sql")
    task_membership_payments_received_merge = int_u.run_query('merge_membership_payments_received',
                                                              "dml/integration/historical/membership_payments_received/membership_payments_received.sql")

    # Membership_fee
    # Membership_fee - Workarea loads
    task_membership_fee_work_source = int_u.run_query("build_membership_fee_work_source",
                                                      "dml/integration/historical/dim_membership_fee/mzp_membership_fee_work_source.sql")
    task_membership_fee_work_transformed = int_u.run_query("build_membership_fee_work_transformed",
                                                           "dml/integration/historical/dim_membership_fee/mzp_membership_fee_work_transformed.sql")
    task_membership_fee_work_final_staging = int_u.run_query("build_membership_fee_work_final_staging",
                                                             "dml/integration/historical/dim_membership_fee/mzp_membership_fee_work_type_2_hist.sql")
    # Membership_fee - Merge Load

    task_membership_fee_merge = int_u.run_query('merge_membership_fee',
                                                "dml/integration/historical/dim_membership_fee/dim_membership_fee.sql")

    # membership_solicitation - Workarea loads
    task_membership_solicitation_work_source = int_u.run_query("build_membership_solicitation_work_source",
                                                               "dml/integration/historical/dim_membership_solicitation/mzp_solicitation_work_source.sql")
    task_membership_solicitation_work_transformed = int_u.run_query("build_membership_solicitation_work_transformed",
                                                                    "dml/integration/historical/dim_membership_solicitation/mzp_solicitation_work_transformed.sql")
    task_membership_solicitation_work_final_staging = int_u.run_query(
        "build_membership_solicitation_work_final_staging",
        "dml/integration/historical/dim_membership_solicitation/mzp_solicitation_work_type_2_hist.sql")

    # membership_solicitation - Merge Load
    task_membership_solicitation_merge = int_u.run_query('merge_membership_solicitation',
                                                         "dml/integration/historical/dim_membership_solicitation/dim_membership_solicitation.sql")

    # Payment Plan

    task_payment_plan_source = int_u.run_query('paymentplan_source',
                                               "dml/integration/historical/dim_membership_payment_plan/mzp_membership_payment_plan_work_source.sql")
    task_payment_plan_transformed = int_u.run_query('paymentplan_transformed',
                                                    "dml/integration/historical/dim_membership_payment_plan/mzp_membership_payment_plan_work_transformed.sql")
    task_payment_plan_stage = int_u.run_query('paymentplan_stage',
                                              "dml/integration/historical/dim_membership_payment_plan/mzp_membership_payment_plan_work_type_2_hist.sql")
    task_payment_plan_merge = int_u.run_query('paymentplan_merge',
                                              "dml/integration/historical/dim_membership_payment_plan/dim_membership_payment_plan.sql")

    # Membership_Payment_Applied_Detail
    task_member_pay_app_detail_work_source = int_u.run_query('build_member_pay_app_detail_work_source',
                                                             "dml/integration/historical/membership_payment_applied_detail/mzp_membership_payment_applied_detail_work_source.sql")
    task_member_pay_app_detail_work_transformed = int_u.run_query('build_member_pay_app_detail_work_transformed',
                                                                  "dml/integration/historical/membership_payment_applied_detail/mzp_membership_payment_applied_detail_work_transformed.sql")
    task_member_pay_app_detail_work_final_staging = int_u.run_query('build_member_pay_app_detail_work_final_staging',
                                                                    "dml/integration/historical/membership_payment_applied_detail/mzp_membership_payment_applied_detail_work_type_2_hist.sql")
    task_membership_pay_app_detail_merge = int_u.run_query('merge_membership_pay_app_detail_merge',
                                                           "dml/integration/historical/membership_payment_applied_detail/membership_payment_applied_detail.sql")

    # Sales_agent_Activity
    task_sales_agent_activity_work_source = int_u.run_query('sales_agent_activity_work_source',
                                                            "dml/integration/historical/sales_agent_activity/mzp_sales_agent_activity_work_source.sql")
    task_sales_agent_activity_work_transformed = int_u.run_query('sales_agent_activity_work_transformed',
                                                                 "dml/integration/historical/sales_agent_activity/mzp_sales_agent_activity_work_transformed.sql")
    task_sales_agent_activity_work_final_staging = int_u.run_query('sales_agent_activity_work_final_staging',
                                                                   "dml/integration/historical/sales_agent_activity/mzp_sales_agent_activity_work_type_2_hist.sql")
    task_sales_agent_activity_merge = int_u.run_query('sales_agent_activity_merge',
                                                      "dml/integration/historical/sales_agent_activity/sales_agent_activity.sql")
    task_sales_agent_activity_membership_solicitation_insert = int_u.run_query(
        'sales_agent_activity_membership_solicitation_insert',
        "dml/integration/historical/sales_agent_activity/sales_agent_activity_membership_solicitation_insert.sql")

    # fact_customer
    #task_fact_customer_work_source = int_u.run_query('fact_customer_work_source',
    #                                                        "dml/integration/incremental/fact_customer/mzp_fact_customer_work_source.sql")
    #task_fact_customer_work_transformed = int_u.run_query('fact_customer_work_transformed',
    #                                                             "dml/integration/incremental/fact_customer/mzp_fact_customer_work_transformed.sql")
    #task_fact_customer_merge = int_u.run_query('fact_customer_merge',
    #                                                  "dml/integration/incremental/fact_customer/fact_customer.sql")

    task_contact_segm_prep_work = int_u.run_query('dim_contact_segmentation_prep_work',
                                                  "dml/integration/historical/dim_contact_segmentation/dim_contact_segmentation_prep_work.sql")
    task_contact_segm_merge = int_u.run_query('dim_contact_segmentation_merge',
                                              "dml/integration/historical/dim_contact_segmentation/dim_contact_segmentation.sql")

    task_contact_segm_sig_prep_work = int_u.run_query('dim_contact_segm_sig_prep_work',
                                                      "dml/integration/historical/dim_contact_segmentation_sigma/dim_contact_segmentation_sigma_prep_work.sql")
    task_contact_segm_sig_merge = int_u.run_query('dim_contact_segm_sig_merge',
                                                  "dml/integration/historical/dim_contact_segmentation_sigma/dim_contact_segmentation_sigma.sql")


    ######################################################################
    # DEPENDENCIES
#contact dependencies
task_name_insert >> task_contact_work_source
task_phone_insert >> task_contact_work_source
task_address_insert >> task_contact_work_source
task_email_insert >> task_contact_work_source
    
task_contact_work_source >> task_contact_work_stage
task_contact_work_target >> task_contact_work_stage

task_contact_work_stage >> task_contact_insert

task_contact_insert >> task_xref_contact_name_merge >> task_contact_merge
task_contact_insert >> task_xref_contact_address_merge >> task_contact_merge
task_contact_insert >> task_xref_contact_phone_merge >> task_contact_merge
task_contact_insert >> task_xref_contact_email_merge >> task_contact_merge
task_contact_insert >> task_dim_contact_source_key_merge >> task_contact_merge

task_contact_merge >> task_contact_match_audit

# membership solicitation
task_membership_solicitation_work_source >> task_membership_solicitation_work_transformed >> task_membership_solicitation_work_final_staging >> task_membership_solicitation_merge

# member auto renewal card
[task_membership_merge,task_name_insert] >> task_member_auto_renewal_card_work_source >> task_member_auto_renewal_card_work_transformed >> task_member_auto_renewal_card_final_stage >> task_merge_member_auto_renewal_card

# Product
task_product_source >> task_product_transformed >> task_product_stage >> task_product_merge

# employee
task_employee_work_source >> task_employee_work_transformed >> task_employee_work_final_staging >> task_employee_merge

# EMPLOYEE ROLE
[task_employee_merge,
 task_membership_office_merge,task_name_insert] >> task_employee_role_work_source >> task_employee_role_work_transformed >> task_employee_role_work_final_staging >> task_employee_role_merge

# office
task_membership_office_work_source >> task_membership_office_work_transformed >> task_membership_office_work_final_staging >> task_membership_office_merge

# Membership
[task_employee_role_merge,
 task_membership_solicitation_merge,task_address_insert] >> task_membership_work_source >> task_membership_work_transformed >> task_membership_work_final_staging >> task_membership_merge

# Member
[task_membership_merge,
 task_employee_role_merge, task_contact_match_audit] >> task_member_work_source >> task_member_work_transformed >> task_member_work_final_staging >> task_member_merge

# Member_rider
[task_member_merge, task_membership_merge, task_employee_role_merge, task_merge_member_auto_renewal_card,
 task_product_merge] >> task_member_rider_work_source >> task_member_rider_work_transformed >> task_member_rider_work_final_staging >> task_member_rider_merge

# comments
[task_membership_merge, task_employee_merge] >> task_membership_comments_source >> task_membership_comment_fkey >> task_membership_comment_stage >> task_membership_comment

# Membership segment
task_membership_merge >> task_membership_segment_source >> task_membership_segment_transformed >> task_membership_segment_stage >> task_membership_segment_merge

# membership billing_summary
task_membership_merge >> task_billing_summary_work_source >> task_billing_summary_work_transformed >> task_billing_summary_work_final_staging >> task_billing_summary_merge

# membership billing detail
[task_billing_summary_merge, task_member_merge, task_product_merge,
 task_membership_fee_merge] >> task_membership_billing_detail_work_source >> task_membership_billing_detail_work_transformed >> task_billing_detail_work_final_staging >> task_billing_detail_merge

# payment_applied summary
[task_membership_merge, task_membership_payments_received_merge, task_membership_office_merge,
 task_employee_merge] >> task_payment_applied_summary_source >> task_payment_applied_summary_transformed >> task_payment_summary_stage >> task_payment_summary_merge >> task_payment_summary_reversal_update

# membership_gl_payments_applied
[task_membership_merge, task_membership_office_merge,
 task_payment_summary_merge] >> task_membership_gl_payments_applied_work_source >> task_membership_gl_payments_applied_work_transformed >> task_membership_gl_payments_applied_work_final_staging >> task_membership_gl_payments_applied_merge

# membership_payments received
[task_membership_merge, task_billing_summary_merge, task_membership_office_merge, task_employee_merge,
 task_merge_member_auto_renewal_card] >> task_membership_payments_received_work_source >> task_membership_payments_received_work_transformed >> task_membership_payments_received_work_final_staging >> task_membership_payments_received_merge

# Membership_fee
[task_member_merge,
 task_product_merge] >> task_membership_fee_work_source >> task_membership_fee_work_transformed >> task_membership_fee_work_final_staging >> task_membership_fee_merge

# Payment Plan
[task_member_rider_merge,
 task_membership_merge] >> task_payment_plan_source >> task_payment_plan_transformed >> task_payment_plan_stage >> task_payment_plan_merge

# membership_payment_applied_detail
[task_payment_summary_merge, task_member_merge,
 task_product_merge] >> task_member_pay_app_detail_work_source >> task_member_pay_app_detail_work_transformed >> task_member_pay_app_detail_work_final_staging >> task_membership_pay_app_detail_merge

# sales_agent_activity
[task_member_merge, task_membership_solicitation_merge, task_membership_office_merge,
 task_membership_pay_app_detail_merge] >> task_sales_agent_activity_work_source >> task_sales_agent_activity_membership_solicitation_insert >> task_sales_agent_activity_work_transformed >> task_sales_agent_activity_work_final_staging >> task_sales_agent_activity_merge

# Fact_customer (commented out for the time being - based Frank's input)
#[task_member_merge, task_membership_merge] >> task_fact_customer_work_source >> task_fact_customer_work_transformed >>  task_fact_customer_merge

# Dim_contact_segmentation
[task_billing_summary_merge, task_member_merge, task_product_merge] >> task_contact_segm_prep_work >> task_contact_segm_merge >> task_contact_segm_sig_prep_work >> task_contact_segm_sig_merge


AllTaskSuccess = EmailOperator(
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    task_id="AllTaskSuccess",
    to=SUCCESS_EMAIL,
    subject="Env: {{var.value.environment}}, DAG: {{params.dag_name}}, Project: {{var.value.INTEGRATION_PROJECT}}",
    html_content='<h3>All Task completed successfully" </h3>')

AllTaskSuccess.set_upstream([task_membership_comment, task_membership_gl_payments_applied_merge, task_sales_agent_activity_merge, task_payment_plan_merge, task_payment_summary_reversal_update, task_billing_detail_merge, task_membership_segment_merge, task_contact_segm_sig_merge])
