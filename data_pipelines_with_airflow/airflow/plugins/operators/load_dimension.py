from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table='',
                 sql_query="",
                 append=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.append = append

    def execute(self, context):
        self.log.info('LoadDimensionOperator starting...')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.append:
            query = 'INSERT INTO %s %s' % (self.table, self.sql_query)
            redshift.run(query)
        else:
            truncate_query = 'TRUNCATE %s' % (self.table)
            redshift.run(del_query)
            query = 'INSERT INTO %s %s' % (self.table, self.sql_query)
            redshift.run(query)
