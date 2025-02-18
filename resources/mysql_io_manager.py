from contextlib import contextmanager
import pandas as pd
from dagster import IOManager, OutputContext, InputContext
from sqlalchemy import create_engine

@contextmanager
def connect_mysql(config):
    conn_info = (
        f"mysql+pymysql://{config['user']}:{config['password']}"
        + f"@{config['host']}:{config['port']}"
        + f"/{config['database']}"
    )
    db_conn = create_engine(conn_info)
    try:
        yield db_conn
    except Exception:
        raise

class MySQLIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        schema, table = context.asset_key.path[-2], context.asset_key.path[-1]
        
        with connect_mysql(self._config) as conn:
            # Write data to MySQL
            obj.to_sql(
                name=table,
                schema=schema,
                con=conn,
                if_exists='replace',
                index=False,
                method='multi',
                chunksize=1000
            )
            
            # Add primary keys if specified in metadata
            if context.metadata and "primary_keys" in context.metadata:
                primary_keys = context.metadata["primary_keys"]
                pk_columns = '`, `'.join(primary_keys)
                pk_name = f"pk_{table}"
                try:
                    conn.execute(
                        f"ALTER TABLE `{schema}`.`{table}` "
                        f"ADD CONSTRAINT `{pk_name}` "
                        f"PRIMARY KEY (`{pk_columns}`)"
                    )
                except Exception as e:
                    context.log.warning(f"Could not add primary key: {str(e)}")

    def load_input(self, context: InputContext) -> pd.DataFrame:
        schema, table = context.asset_key.path[-2], context.asset_key.path[-1]
        with connect_mysql(self._config) as conn:
            sql = f"SELECT * FROM `{schema}`.`{table}`"
            return pd.read_sql(sql, conn)

    def extract_data(self, sql: str) -> pd.DataFrame:
        with connect_mysql(self._config) as db_conn:
            pd_data = pd.read_sql_query(sql, db_conn)
            return pd_data