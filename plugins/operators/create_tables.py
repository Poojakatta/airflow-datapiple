from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class CreateTablesOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table,
                 redshift_conn_id="redshift",
                 select_sql="",
                 *args, **kwargs):

        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.select_sql = select_sql

    def execute(self, context):        
        
        redshift_hook = PostgresHook(self.redshift_conn_id)

        self.log.info('DROP {} started..'.format(self.table))  

        sql = "DROP TABLE IF EXISTS {}".format(self.table)
        redshift_hook.run(sql)

        self.log.info('DROP {} DONE..'.format(self.table))  

        self.log.info('Creating {} started..'.format(self.table))       

        redshift_hook.run(self.select_sql)

        self.log.info('Creating {} is done..'.format(self.table))       
