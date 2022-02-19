from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "", 
                 aws_credentials_id="", 
                 sql_query = "", 
                 delimiter = ",", 
                 ignore_headers = 1,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id
        self.sql_query = sql_query

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        self.log.info("Upserting data to Redshift")
        formatted_sql = LoadFactOperator.copy_sql.format(
            self.sql_query, 
            credentials.access_key, 
            credentials.secret_key, 
            self.ignore_headers, 
            self.delimiter
        )
        redshift.run(formatted_sql)
