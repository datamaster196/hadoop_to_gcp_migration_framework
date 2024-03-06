#######################################################################################################################
##  DAG_NAME :  setup_env_integrate
##  PURPOSE :   Executes DDL to set up integration layer
##  PREREQUISITE DAGs : DROP integration tables
##                      UTIL-DROP-INTEGRATION-*-TABLES
#######################################################################################################################


from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.models import Variable
from utils import ingestion_utilities as iu
from utils import integration_utilities as int_u
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

SUCCESS_EMAIL = Variable.get("email_recipient_success", default_var='') or None
FAIL_EMAIL = Variable.get("email_recipient_failure", default_var='') or None

SOURCE_DB_NAMES = ['epic', 'd3', 'mzp']
DAG_TITLE = 'setup_env_integrate'
INGESTION_PROJECT = '{{ var.value.INGESTION_PROJECT }}'
INTEGRATION_PROJECT = '{{ var.value.INTEGRATION_PROJECT }}'


def bq_make_dataset(source_db, project=f'{{ var.value.INTEGRATION_PROJECT }}'):
    task_id = f'create-dataset-{iu.ENV}-{source_db}'
    cmd = f"""bq ls "{project}:{source_db}" || bq mk "{project}:{source_db}" """

    return BashOperator(task_id=task_id, bash_command=cmd)

def gcs_make_bucket(dag, bucket_name):
    task_id = f'create_bucket_{bucket_name}'
    cmd = f" VAR=$(( gsutil ls gs://{bucket_name})| wc -l) 2> null &&  \n \
    if [ $VAR -eq 0 ]  \n \
       then VAR1=$( gsutil mb -p {iu.INGESTION_PROJECT} -l {iu.REGION} -b off gs://{bucket_name}) ; echo $VAR1 \n \
       else echo \"The bucket (gs://{bucket_name}) already exists\"  \n \
    fi"

    task = BashOperator(task_id=task_id, bash_command=cmd, dag=dag)

    return task


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
    'execution_timeout': timedelta(minutes=60),
    'params': {
        'dag_name': DAG_TITLE
    }
}

with DAG(DAG_TITLE, default_args=default_args, schedule_interval=None, max_active_runs=1,
         template_searchpath='/home/airflow/gcs/dags/sql/') as dag:
    
    # Build Bucket
    create_bucket_adw_outgoing = gcs_make_bucket(dag, f'{int_u.INTEGRATION_PROJECT}-outgoing'.lower())    
    
    ###adw datasets table definitions

    audit_contact_match = BigQueryOperator(
        task_id='audit_contact_match',
        sql='ddl/adw/audit_contact_match.sql',
        use_legacy_sql=False)

    dim_aca_office = BigQueryOperator(
        task_id='dim_aca_office',
        sql='ddl/adw/dim_aca_office.sql',
        use_legacy_sql=False)

    dim_business_line = BigQueryOperator(
        task_id='dim_business_line',
        sql='ddl/adw/dim_business_line.sql',
        use_legacy_sql=False)

    dim_comment = BigQueryOperator(
        task_id='dim_comment',
        sql='ddl/adw/dim_comment.sql',
        use_legacy_sql=False)

    dim_contact_source_key = BigQueryOperator(
        task_id='dim_contact_source_key',
        sql='ddl/adw/dim_contact_source_key.sql',
        use_legacy_sql=False)

    dim_demogr_insurance_score = BigQueryOperator(
        task_id='dim_demogr_insurance_score',
        sql='ddl/adw/dim_demogr_insurance_score.sql',
        use_legacy_sql=False)

    dim_demogr_person = BigQueryOperator(
        task_id='dim_demogr_person',
        sql='ddl/adw/dim_demogr_person.sql',
        use_legacy_sql=False)

    dim_employee_role = BigQueryOperator(
        task_id='dim_employee_role',
        sql='ddl/adw/dim_employee_role.sql',
        use_legacy_sql=False)

    dim_member_auto_renewal_card = BigQueryOperator(
        task_id='dim_member_auto_renewal_card',
        sql='ddl/adw/dim_member_auto_renewal_card.sql',
        use_legacy_sql=False)

    dim_member_rider = BigQueryOperator(
        task_id='dim_member_rider',
        sql='ddl/adw/dim_member_rider.sql',
        use_legacy_sql=False)

    dim_member = BigQueryOperator(
        task_id='dim_member',
        sql='ddl/adw/dim_member.sql',
        use_legacy_sql=False)

    dim_membership_fee = BigQueryOperator(
        task_id='dim_membership_fee',
        sql='ddl/adw/dim_membership_fee.sql',
        use_legacy_sql=False)

    dim_membership_marketing_segmentation = BigQueryOperator(
        task_id='dim_membership_marketing_segmentation',
        sql='ddl/adw/dim_membership_marketing_segmentation.sql',
        use_legacy_sql=False)

    dim_membership_payment_plan = BigQueryOperator(
        task_id='dim_membership_payment_plan',
        sql='ddl/adw/dim_membership_payment_plan.sql',
        use_legacy_sql=False)

    dim_membership_solicitation = BigQueryOperator(
        task_id='dim_membership_solicitation',
        sql='ddl/adw/dim_membership_solicitation.sql',
        use_legacy_sql=False)

    dim_membership = BigQueryOperator(
        task_id='dim_membership',
        sql='ddl/adw/dim_membership.sql',
        use_legacy_sql=False)

    dim_product_category = BigQueryOperator(
        task_id='dim_product_category',
        sql='ddl/adw/dim_product_category.sql',
        use_legacy_sql=False)

    dim_product = BigQueryOperator(
        task_id='dim_product',
        sql='ddl/adw/dim_product.sql',
        use_legacy_sql=False)

    dim_vendor = BigQueryOperator(
        task_id='dim_vendor',
        sql='ddl/adw/dim_vendor.sql',
        use_legacy_sql=False)

    membership_billing_detail = BigQueryOperator(
        task_id='membership_billing_detail',
        sql='ddl/adw/membership_billing_detail.sql',
        use_legacy_sql=False)

    membership_billing_summary = BigQueryOperator(
        task_id='membership_billing_summary',
        sql='ddl/adw/membership_billing_summary.sql',
        use_legacy_sql=False)

    membership_gl_payments_applied = BigQueryOperator(
        task_id='membership_gl_payments_applied',
        sql='ddl/adw/membership_gl_payments_applied.sql',
        use_legacy_sql=False)

    membership_payment_applied_detail = BigQueryOperator(
        task_id='membership_payment_applied_detail',
        sql='ddl/adw/membership_payment_applied_detail.sql',
        use_legacy_sql=False)

    membership_payment_applied_summary = BigQueryOperator(
        task_id='membership_payment_applied_summary',
        sql='ddl/adw/membership_payment_applied_summary.sql',
        use_legacy_sql=False)

    membership_payments_received = BigQueryOperator(
        task_id='membership_payments_received',
        sql='ddl/adw/membership_payments_received.sql',
        use_legacy_sql=False)

    sales_agent_activity = BigQueryOperator(
        task_id='sales_agent_activity',
        sql='ddl/adw/sales_agent_activity.sql',
        use_legacy_sql=False)

    xref_contact_address = BigQueryOperator(
        task_id='xref_contact_address',
        sql='ddl/adw/xref_contact_address.sql',
        use_legacy_sql=False)

    xref_contact_email = BigQueryOperator(
        task_id='xref_contact_email',
        sql='ddl/adw/xref_contact_email.sql',
        use_legacy_sql=False)

    xref_contact_name = BigQueryOperator(
        task_id='xref_contact_name',
        sql='ddl/adw/xref_contact_name.sql',
        use_legacy_sql=False)

    xref_contact_phone = BigQueryOperator(
        task_id='xref_contact_phone',
        sql='ddl/adw/xref_contact_phone.sql',
        use_legacy_sql=False)

    insurance_policy = BigQueryOperator(
        task_id='insurance_policy',
        sql='ddl/adw/insurance_policy.sql',
        use_legacy_sql=False)

    insurance_policy_line = BigQueryOperator(
        task_id='insurance_policy_line',
        sql='ddl/adw/insurance_policy_line.sql',
        use_legacy_sql=False)

    insurance_customer_role = BigQueryOperator(
        task_id='insurance_customer_role',
        sql='ddl/adw/insurance_customer_role.sql',
        use_legacy_sql=False)

    dim_state = BigQueryOperator(
        task_id='dim_state',
        sql='ddl/adw/dim_state.sql',
        use_legacy_sql=False)

    dim_address = BigQueryOperator(
        task_id='dim_address',
        sql='ddl/adw_pii/dim_address.sql',
        use_legacy_sql=False)

    dim_contact_info = BigQueryOperator(
        task_id='dim_contact_info',
        sql='ddl/adw_pii/dim_contact_info.sql',
        use_legacy_sql=False)

    dim_email = BigQueryOperator(
        task_id='dim_email',
        sql='ddl/adw_pii/dim_email.sql',
        use_legacy_sql=False)

    dim_employee = BigQueryOperator(
        task_id='dim_employee',
        sql='ddl/adw_pii/dim_employee.sql',
        use_legacy_sql=False)

    dim_name = BigQueryOperator(
        task_id='dim_name',
        sql='ddl/adw_pii/dim_name.sql',
        use_legacy_sql=False)

    dim_phone = BigQueryOperator(
        task_id='dim_phone',
        sql='ddl/adw_pii/dim_phone.sql',
        use_legacy_sql=False)

    dim_contact_segmentation = BigQueryOperator(
        task_id='dim_contact_segmentation',
        sql='ddl/adw/dim_contact_segmentation.sql',
        use_legacy_sql=False)

    fact_contact_event_optout = BigQueryOperator(
        task_id='fact_contact_event_optout',
        sql='ddl/adw/fact_contact_event_optout.sql',
        use_legacy_sql=False)

    dim_contact_optout = BigQueryOperator(
        task_id='dim_contact_optout',
        sql='ddl/adw/dim_contact_optout.sql',
        use_legacy_sql=False)

    fact_sales_header = BigQueryOperator(
        task_id='fact_sales_header',
        sql='ddl/adw/fact_sales_header.sql',
        use_legacy_sql=False)

    fact_roadside_service_call    = BigQueryOperator(
        task_id='fact_roadside_service_call',
        sql='ddl/adw/fact_roadside_service_call.sql',
        use_legacy_sql=False)

    fact_sales_detail = BigQueryOperator(
        task_id='fact_sales_detail',
        sql='ddl/adw/fact_sales_detail.sql',
        use_legacy_sql=False)

    cdl_recipient = BigQueryOperator(
        task_id='cdl_recipient',
        sql='ddl/cdl/cdl_recipient.sql',
        use_legacy_sql=False)

    rap_service_call_payments = BigQueryOperator(
        task_id='rap_service_call_payments',
        sql='ddl/adw/rap_service_call_payments.sql',
        use_legacy_sql=False)

    # insurance_agency = BigQueryOperator(
    #    task_id='insurance_agency',
    #    sql='ddl/insurance/insurance_agency.sql',
    #    use_legacy_sql=False)
    #
    # insurance_agent = BigQueryOperator(
    #    task_id='insurance_agent',
    #    sql='ddl/insurance/insurance_agent.sql',
    #    use_legacy_sql=False)
    #
    # insurance_client = BigQueryOperator(
    #    task_id='insurance_client',
    #    sql='ddl/insurance/insurance_client.sql',
    #    use_legacy_sql=False)
    #
    # insurance_coverage = BigQueryOperator(
    #    task_id='insurance_coverage',
    #    sql='ddl/insurance/insurance_coverage.sql',
    #    use_legacy_sql=False)
    #
    # insurance_driver = BigQueryOperator(
    #    task_id='insurance_driver',
    #    sql='ddl/insurance/insurance_driver.sql',
    #    use_legacy_sql=False)
    #
    # insurance_line = BigQueryOperator(
    #    task_id='insurance_line',
    #    sql='ddl/insurance/insurance_line.sql',
    #    use_legacy_sql=False)
    #
    # insurance_monthly_policy_snapshot = BigQueryOperator(
    #    task_id='insurance_monthly_policy_snapshot',
    #    sql='ddl/insurance/insurance_monthly_policy_snapshot.sql',
    #    use_legacy_sql=False)
    #
    # insurance_policy_transactions = BigQueryOperator(
    #    task_id='insurance_policy_transactions',
    #    sql='ddl/insurance/insurance_policy_transactions.sql',
    #    use_legacy_sql=False)
    #
    # insurance_policy = BigQueryOperator(
    #    task_id='insurance_policy',
    #    sql='ddl/insurance/insurance_policy.sql',
    #    use_legacy_sql=False)
    #
    # insurance_quote = BigQueryOperator(
    #    task_id='insurance_quote',
    #    sql='ddl/insurance/insurance_quote.sql',
    #    use_legacy_sql=False)
    #
    # insurance_vehicle = BigQueryOperator(
    #    task_id='insurance_vehicle',
    #    sql='ddl/insurance/insurance_vehicle.sql',
    #    use_legacy_sql=False)

    # fact_customer = BigQueryOperator(
    #    task_id='fact_customer',
    #    sql='ddl/adw/fact_customer.sql',
    #    use_legacy_sql=False)

    format_phone_number = BigQueryOperator(
        task_id='format_phone_number',
        sql='ddl/udfs/format_phone_number.sql',
        use_legacy_sql=False)

    parse_member_id = BigQueryOperator(
        task_id='parse_member_id',
        sql='ddl/udfs/parse_member_id.sql',
        use_legacy_sql=False)

    parse_zipcode = BigQueryOperator(
        task_id='parse_zipcode',
        sql='ddl/udfs/parse_zipcode.sql',
        use_legacy_sql=False)

    standardize_string = BigQueryOperator(
        task_id='standardize_string',
        sql='ddl/udfs/standardize_string.sql',
        use_legacy_sql=False)

    bq_create_udf_numeric_suffix_to_alpha = BigQueryOperator(
        task_id='numeric_suffix_to_alpha',
        #### Don't forget to add {{ params.<<whatever>> }} to your query in the file
        sql='udf/bq_udf_numeric_suffix_to_alpha.sql',
        use_legacy_sql=False
    )
    bq_udf_remove_selected_punctuation = BigQueryOperator(
        task_id='remove_selected_punctuation',
        #### Don't forget to add {{ params.<<whatever>> }} to your query in the file
        sql='udf/bq_udf_remove_selected_punctuation.sql',
        use_legacy_sql=False
    )
    bq_udf_diacritic_to_standard = BigQueryOperator(
        task_id='diacritic_to_standard',
        #### Don't forget to add {{ params.<<whatever>> }} to your query in the file
        sql='udf/bq_udf_diacritic_to_standard.sql',
        use_legacy_sql=False
    )

    bq_udf_hash_address = BigQueryOperator(
        task_id='hash_address',
        #### Don't forget to add {{ params.<<whatever>> }} to your query in the file
        sql='udf/bq_udf_hash_address.sql',
        use_legacy_sql=False
    )

    bq_udf_hash_email = BigQueryOperator(
        task_id='hash_email',
        #### Don't forget to add {{ params.<<whatever>> }} to your query in the file
        sql='udf/bq_udf_hash_email.sql',
        use_legacy_sql=False
    )

    bq_udf_hash_name = BigQueryOperator(
        task_id='hash_name',
        #### Don't forget to add {{ params.<<whatever>> }} to your query in the file
        sql='udf/bq_udf_hash_name.sql',
        use_legacy_sql=False
    )

    bq_udf_hash_phone = BigQueryOperator(
        task_id='hash_phone',
        #### Don't forget to add {{ params.<<whatever>> }} to your query in the file
        sql='udf/bq_udf_hash_phone.sql',
        use_legacy_sql=False
    )

    ########
    # Populate Stubbed records #
    ########

    bq_populate_dim_business_line = BigQueryOperator(
        task_id='dim_business_line_stubbed_records',
        #### Don't forget to add {{ params.<<whatever>> }} to your query in the file
        sql='stubbed_data/populate_dim_business_line.sql',
        use_legacy_sql=False
    )

    bq_populate_dim_vendor = BigQueryOperator(
        task_id='dim_vendor_stubbed_records',
        #### Don't forget to add {{ params.<<whatever>> }} to your query in the file
        sql='stubbed_data/populate_dim_vendor.sql',
        use_legacy_sql=False
    )

    bq_populate_dim_product_category = BigQueryOperator(
        task_id='dim_product_category_stubbed_records',
        #### Don't forget to add {{ params.<<whatever>> }} to your query in the file
        sql='stubbed_data/populate_dim_product_category.sql',
        use_legacy_sql=False
    )

    bq_populate_dim_aca_office = BigQueryOperator(
        task_id='dim_aca_office_stubbed_records',
        #### Don't forget to add {{ params.<<whatever>> }} to your query in the file
        sql='stubbed_data/populate_dim_aca_office.sql',
        use_legacy_sql=False
    )

    # Build ingestion Datasets
    make_dataset_udfs = bq_make_dataset('udfs', f'{INTEGRATION_PROJECT}')
    make_dataset_adw_work = bq_make_dataset('adw_work', f'{INTEGRATION_PROJECT}')
    make_dataset_adw = bq_make_dataset('adw', f'{INTEGRATION_PROJECT}')
    make_dataset_adw_pii = bq_make_dataset('adw_pii', f'{INTEGRATION_PROJECT}')
    make_dataset_cdl = bq_make_dataset('cdl', f'{INTEGRATION_PROJECT}')
    # make_dataset_insurance = bq_make_dataset('insurance', f'{INTEGRATION_PROJECT}')

    # Build udfs in Dataset udfs
    [make_dataset_udfs] >> format_phone_number
    [make_dataset_udfs] >> parse_member_id
    [make_dataset_udfs] >> parse_zipcode
    [make_dataset_udfs] >> standardize_string
    [make_dataset_udfs] >> bq_create_udf_numeric_suffix_to_alpha
    [make_dataset_udfs] >> bq_udf_remove_selected_punctuation
    [make_dataset_udfs] >> bq_udf_diacritic_to_standard
    [make_dataset_udfs] >> bq_udf_hash_address
    [make_dataset_udfs] >> bq_udf_hash_email
    [make_dataset_udfs] >> bq_udf_hash_name
    [make_dataset_udfs] >> bq_udf_hash_phone

    make_dataset_adw_pii >> [dim_address, dim_contact_info, dim_email, dim_employee, dim_name, dim_phone]

    make_dataset_adw >> [audit_contact_match, dim_aca_office, dim_business_line, dim_comment, dim_contact_source_key,
                         dim_demogr_insurance_score, dim_demogr_person, dim_employee_role, dim_member_auto_renewal_card,
                         dim_member_rider, dim_member, dim_membership_fee, dim_membership_marketing_segmentation,
                         dim_membership_payment_plan, dim_membership_solicitation, dim_membership, dim_product_category,
                         dim_product, dim_vendor, membership_billing_detail, membership_billing_summary,
                         membership_gl_payments_applied, membership_payment_applied_detail,
                         membership_payment_applied_summary, membership_payments_received, sales_agent_activity,
                         xref_contact_address, xref_contact_email, xref_contact_name, xref_contact_phone,
                         insurance_policy, insurance_policy_line, insurance_customer_role, dim_state,
                         dim_contact_segmentation,fact_contact_event_optout,dim_contact_optout,
                         fact_sales_header,fact_roadside_service_call,fact_sales_detail,rap_service_call_payments]

    make_dataset_cdl >> cdl_recipient

    # make_dataset_adw >> dim_club
    # make_dataset_adw >> fact_customer

    # make_dataset_insurance >> insurance_agency
    # make_dataset_insurance >> insurance_agent
    # make_dataset_insurance >> insurance_client
    # make_dataset_insurance >> insurance_coverage
    # make_dataset_insurance >> insurance_driver
    # make_dataset_insurance >> insurance_line
    # make_dataset_insurance >> insurance_monthly_policy_snapshot
    # make_dataset_insurance >> insurance_policy_transactions
    # make_dataset_insurance >> insurance_policy
    # make_dataset_insurance >> insurance_quote
    # make_dataset_insurance >> insurance_vehicle

    # Populate Stubbed Records

    dim_product_category >> bq_populate_dim_product_category
    dim_business_line >> bq_populate_dim_business_line
    dim_vendor >> bq_populate_dim_vendor
    dim_aca_office >> bq_populate_dim_aca_office

    AllTaskSuccess = EmailOperator(
        dag=dag,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        task_id="AllTaskSuccess",
        to=SUCCESS_EMAIL,
        subject="Env: {{var.value.environment}}, DAG: {{params.dag_name}}, Project: {{var.value.INTEGRATION_PROJECT}}",
        html_content='<h3>All Task completed successfully" </h3>')

    AllTaskSuccess.set_upstream(
        [format_phone_number, parse_member_id, parse_zipcode, standardize_string, bq_create_udf_numeric_suffix_to_alpha,
         bq_create_udf_numeric_suffix_to_alpha, bq_udf_remove_selected_punctuation, bq_udf_diacritic_to_standard,
         bq_udf_hash_address, bq_udf_hash_email, bq_udf_hash_name, bq_udf_hash_phone, make_dataset_adw_work,
         dim_employee, dim_address, dim_contact_info, dim_email, dim_name, dim_phone, audit_contact_match, dim_comment,
         dim_contact_source_key, dim_demogr_insurance_score, dim_demogr_person, dim_employee_role,
         dim_member_auto_renewal_card, dim_member_rider, dim_member, dim_membership_fee,
         dim_membership_marketing_segmentation, dim_membership_payment_plan, dim_membership_solicitation,
         dim_membership, dim_product, membership_billing_detail, membership_billing_summary,
         membership_gl_payments_applied, membership_payment_applied_detail, membership_payment_applied_summary,
         membership_payments_received, sales_agent_activity, xref_contact_address, xref_contact_email,
         xref_contact_name, xref_contact_phone, insurance_policy, insurance_policy_line, insurance_customer_role,
         dim_state, dim_contact_segmentation,fact_contact_event_optout,dim_contact_optout,fact_sales_header,fact_roadside_service_call,fact_sales_detail,
         cdl_recipient,rap_service_call_payments,
         bq_populate_dim_product_category, bq_populate_dim_business_line,
         bq_populate_dim_vendor, bq_populate_dim_aca_office])