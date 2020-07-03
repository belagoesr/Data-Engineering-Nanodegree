from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        TIMEFORMAT as 'epochmillisecs'
        JSON '{}'
    """

    @apply_defaults
    def __init__(self, 
                 redshift_conn_id="redshift", 
                 table='',
                 region='us-west-2',
                 s3_bucket='',
                 s3_key='',
                 json_path='',
                 aws_credentials_id='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.region = region
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.aws_credentials_id = aws_credentials_id
        
        
    def execute(self, context):
        self.log.info('StageToRedshiftOperator starting...', self.s3_bucket, self.json_path)
        redshift = PostgresHook(self.redshift_conn_id)
        path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        if self.json_path != "auto":
            json_path = "s3://{}/{}".format(self.s3_bucket, self.json_path)
        else:
            json_path = self.json_path
            
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
            
        query = StageToRedshiftOperator.copy_sql.format(
            self.table,
            path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            json_path,
        )
        redshift.run(query)

        self.log.info(f'Copied {self.table} to Redshift successfully...')
        





