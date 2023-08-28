from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table,
                 redshift_conn_id="redshift",
                 select_sql="",
                 mode="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.select_sql = select_sql
        self.mode = mode

    def execute(self, context):
        self.log.info('Loading Dimension table {} started...'.format(self.table))
        redshift_hook = PostgresHook(self.redshift_conn_id)

        if self.mode == "truncate":
            delete_sql = f"DELETE FROM {self.table};"
            redshift_hook.run(delete_sql)
            self.log.info("\ndelete table is done.")

        sql = """INSERT INTO {table} {select_sql};""".format(table=self.table, select_sql=self.select_sql)

        self.log.info(f"loading data into {self.table}..")

        redshift_hook.run(sql)
        self.log.info("loading is done.")
