from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    insert_sql = "INSERT INTO {} {}"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 aws_credentials_id = '',
                 table = '',
                 sql = '',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        self.log.info('Loading data into fact table')
        formatted_sql = LoadFactOperator.insert_sql.format(
            self.table,
            self.sql
        )
        
        redshift_hook.run(formatted_sql)
        self.log.info('LoadFactOperator now implemented')
        
        