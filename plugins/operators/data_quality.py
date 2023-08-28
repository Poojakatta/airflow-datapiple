from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 tables=[],
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.dq_checks = dq_checks

    def execute(self, context):
        self.log.info('Data Quality check starting.....')
        redshift_hook = PostgresHook(self.redshift_conn_id)

        error_count = 0
        failing_checks = []

        for check in self.dq_checks:
            sql_query = check.get("check_sql")
            expected_result = check.get("expected_result")
            table = check.get("table")

            records_query = redshift_hook.get_records(sql_query)[0]

            if expected_result != records_query[0]:
                error_count += 1
                failing_checks.append(sql_query)

            if error_count > 0:
                self.log.info(failing_checks)
                raise ValueError("Data validation failed for table - {}".format(table))

        self.log.info('Data Quality check completed successfully')