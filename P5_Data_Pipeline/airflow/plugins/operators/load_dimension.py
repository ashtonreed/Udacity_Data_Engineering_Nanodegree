from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    insert_sql = "INSERT INTO {} {}"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 aws_credentials_id = '',
                 table = '',
                 sql = '',
                 truncate = '',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.sql = sql
        self.truncate = truncate

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)

        if self.truncate:
            self.log.info(f"Truncating {self.table} dimension table")
            redshift_hook.run(f"TRUNCATE TABLE {self.table}")
        
        self.log.info(f"Loading data into {self.table} dimension table")
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.sql
        )
        
        redshift_hook.run(formatted_sql)
        self.log.info(f"LoadDimensionOperator now implemented for {self.table} table")