"""
Module containing functionality for authenticating with PostgreSQL databases
"""
import psycopg
from typing import Any, Dict, Optional, Union
from pydantic import SecretStr
from prefect.blocks.core import Block
from psycopg import connect


class PostgresDatabaseCredentials(Block):
    """
    Block used to manage authentication with a database.
    Attributes:
        database: The name of the database to use.
        username: The user name used to authenticate.
        password: The password used to authenticate.
        host: The host address of the database.
        port: The port to connect to the database.
        connect_args: The options which will be passed directly to the
            DBAPI's connect() method as additional keyword arguments.
    Example:
        Load stored database credentials:
        ```python
        from prefect_postgres import DatabaseCredentials
        database_block = DatabaseCredentials.load("BLOCK_NAME")
        ```
    """

    _block_type_name = "Postgres Database Credentials"
    _logo_url = "https://images.ctfassets.net/gm98wzqotmnx/7G8c2Zz4j0yyhXqQ44SI2A/eee9ba482dd6b61862b588b6fd28ad81/PostgreSQL-Logo.png?h=250"  # noqa

    username: Optional[str] = None
    password: Optional[SecretStr] = None
    database: Optional[str] = None
    host: Optional[str] = None
    port: Optional[str] = None
    connect_args: Optional[Dict[str, Any]] = None

    def get_connection(self) -> connect:
        """
        Returns an authenticated connection object that can be
        used to query from databases.
        Returns:
            The authenticated Psycopg3 AsyncConnection.
        Examples:
            Create an asynchronous connection to PostgreSQL using URL params.
            ```python
            from prefect import flow
            from prefect_postgres import DatabaseCredentials
            @flow
            def postgres_credentials_flow():
                postgres_credentials = DatabaseCredentials(
                    username="prefect",
                    password="prefect_password",
                    database="postgres",
                    host="localhost",
                    port=5432
                )
                conn = postgres_credentials.get_connection()
                with conn.cursor() as cur:
                    cur.execute("CREATE TABLE test (id serial PRIMARY KEY,num integer,data text)")
            postgres_credentials_flow()
            ```
        """
        return psycopg.connect(
            database=self.database,
            user=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
            connect_args=self.connect_args,
        )
