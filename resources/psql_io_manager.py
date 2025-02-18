from contextlib import contextmanager
from datetime import datetime
import pandas as pd
from dagster import IOManager, OutputContext, InputContext
from sqlalchemy import create_engine, text

@contextmanager
def connect_psql(config):
    conn_info = (
        f"postgresql+psycopg2://{config['user']}:{config['password']}"
        + f"@{config['host']}:{config['port']}"
        + f"/{config['database']}"
    )
    db_conn = create_engine(conn_info)
    try:
        yield db_conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        raise
    finally:
        db_conn.dispose()

class PostgreSQLIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def _create_table_if_not_exists(self, connection, schema: str, table: str, df: pd.DataFrame, columns: list):
        # Map pandas dtypes to PostgreSQL types
        dtype_mapping = {
            'object': 'TEXT',
            'int64': 'BIGINT',
            'float64': 'DOUBLE PRECISION',
            'datetime64[ns]': 'TIMESTAMP',
            'bool': 'BOOLEAN',
            'category': 'TEXT'
        }
        
        # Create schema if not exists
        try:
            connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        except Exception as e:
            print(f"Error creating schema {schema}: {e}")
            raise
        
        # Generate column definitions
        column_defs = []
        for col in columns:
            sql_type = dtype_mapping.get(str(df[col].dtype), 'TEXT')
            column_defs.append(f"\"{col}\" {sql_type}")
        
        # Create table if not exists
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            {', '.join(column_defs)}
        )
        """
        try:
            connection.execute(text(create_table_sql))
            print(f"Table {schema}.{table} created or already exists.")
        except Exception as e:
            print(f"Error creating table {schema}.{table}: {e}")
            raise

    def load_input(self, context: InputContext) -> pd.DataFrame:
        schema, table = context.asset_key.path[-2], context.asset_key.path[-1]
        
        with connect_psql(self._config) as db_conn:
            try:
                print(f"Loading data from {schema}.{table}...")
                return pd.read_sql_table(
                    table,
                    db_conn,
                    schema=schema
                )
            except Exception as e:
                print(f"Error loading {schema}.{table}: {str(e)}")
                return pd.DataFrame()  # Return empty DataFrame if loading fails

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        schema, table = context.asset_key.path[-2], context.asset_key.path[-1]
        tmp_tbl = f"{table}_tmp_{datetime.now().strftime('%Y_%m_%d')}"

        with connect_psql(self._config) as db_conn:
            primary_keys = (context.metadata or {}).get("primary_keys", [])
            ls_columns = (context.metadata or {}).get("columns", obj.columns.tolist())

            with db_conn.connect() as connection:
                # Create original table if not exists
                try:
                    self._create_table_if_not_exists(
                        connection, 
                        schema, 
                        table, 
                        obj[ls_columns], 
                        ls_columns
                    )
                except Exception as e:
                    print(f"Error ensuring table {schema}.{table} exists: {e}")
                    raise
                
                # Create temp table
                try:
                    connection.execute(
                        text(f"CREATE TEMP TABLE {tmp_tbl} (LIKE {schema}.{table} INCLUDING ALL)")
                    )
                    print(f"Temporary table {tmp_tbl} created.")
                except Exception as e:
                    print(f"Error creating temp table {tmp_tbl}: {e}")
                    raise

                # Insert data into temp table
                try:
                    obj[ls_columns].to_sql(
                        name=tmp_tbl,
                        con=db_conn,
                        schema=schema,
                        if_exists="replace",
                        index=False,
                        chunksize=10000,
                        method="multi",
                    )
                    print(f"Data inserted into temporary table {tmp_tbl}.")
                except Exception as e:
                    print(f"Error inserting data into temp table {tmp_tbl}: {e}")
                    raise

                # Check data in temp table
                try:
                    result = connection.execute(text(f"SELECT COUNT(*) FROM {tmp_tbl}"))
                    for row in result:
                        print(f"Temp table {tmp_tbl} contains {row[0]} records.")
                except Exception as e:
                    print(f"Error checking data in temp table {tmp_tbl}: {e}")
                    raise

                # Upsert data với transaction rõ ràng
                try:
                    # Bắt đầu transaction
                    connection.execute(text("BEGIN"))
                    
                    if primary_keys:
                        conditions = " AND ".join([
                            f"{schema}.{table}.\"{k}\" = {schema}.{tmp_tbl}.\"{k}\""
                            for k in primary_keys
                        ])
                        
                        # Thực hiện DELETE
                        connection.execute(text(f"""
                            DELETE FROM {schema}.{table}
                            USING {schema}.{tmp_tbl}
                            WHERE {conditions}
                        """))
                        
                        # Thực hiện INSERT
                        connection.execute(text(f"""
                            INSERT INTO {schema}.{table}
                            SELECT * FROM {schema}.{tmp_tbl}
                        """))
                    else:
                        # Truncate và insert với CASCADE
                        connection.execute(text(f"TRUNCATE TABLE {schema}.{table} CASCADE"))
                        connection.execute(text(f"""
                            INSERT INTO {schema}.{table}
                            SELECT * FROM {schema}.{tmp_tbl}
                        """))
                    
                    # Drop temp table
                    connection.execute(text(f"DROP TABLE IF EXISTS {schema}.{tmp_tbl}"))
                    
                    # Commit transaction
                    connection.execute(text("COMMIT"))
                    print(f"Data successfully upserted into {schema}.{table}.")

                except Exception as e:
                    # Rollback nếu có lỗi
                    connection.execute(text("ROLLBACK"))
                    print(f"Error during upsert operation: {e}")
                    try:
                        connection.execute(text(f"DROP TABLE IF EXISTS {schema}.{tmp_tbl}"))
                    except Exception as cleanup_error:
                        print(f"Error during cleanup: {cleanup_error}")
                    raise