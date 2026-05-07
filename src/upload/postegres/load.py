import psycopg
from psycopg_pool import AsyncConnectionPool
from psycopg.rows import TupleRow
from psycopg import AsyncConnection
import psycopg_pool
from pipeline.processors.indicator import StagingData
import logging

logger = logging.getLogger(__name__)


class LoadDatabase:
    def __init__(
        self,
        pool: AsyncConnectionPool[AsyncConnection[TupleRow]],
    ) -> None:
        self.pool = pool

    async def create_table(self):
        """Create table if not exists"""
        async with self.pool.connection() as aconn:
            async with aconn:
                async with aconn.cursor() as cur:
                    await cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS staging_data (
                        id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                        date DATE,
                        year INTEGER,
                        source TEXT,
                        code TEXT,
                        indicator TEXT,
                        value float,
                        country TEXT,
                        category TEXT,
                        frequency TEXT,
                        method TEXT,
                        unit TEXT,
                        description TEXT,
                        processed TIMESTAMPTZ,
                        UNIQUE (date, source, code, country, frequency)
                        )
                        """
                    )

    async def load_data(self, data: StagingData):
        """Load Data"""
        # NOTE: This is a simple implementation for loading data to Postgres, it can be improved by using connection pooling and handling exceptions accordingly.
        # Also, it can be refactored to use an ORM like SQLAlchemy for better maintainability and scalability.
        # ConectionPool management can be implemented using psycopg's connection pool or using an external library like SQLAlchemy's connection pool.
        # This will help to manage database connections efficiently and improve performance when handling multiple requests.
        # make DDL run only once, by checking if the table exists before creating it.
        # This can be done by querying the information_schema or using a flag in the application to track if the table has been created.

        async with self.pool.connection() as aconn:  # create AsyncConnection from pool
            async with aconn:  # ensure transaction is properly closed after use
                async with aconn.cursor() as acur:  # create AsyncCursor from connection
                    rows = [
                        (
                            item.date,
                            item.year,
                            item.source,
                            item.code,
                            item.indicator,
                            item.value,
                            item.country,
                            item.category,
                            item.frequency,
                            item.method,
                            item.unit,
                            item.description,
                            item.processed,
                        )
                        for item in data.staging_result
                    ]
                    try:
                        await acur.executemany(
                            """
                                    INSERT INTO staging_data (
                                        date, year, source, code, indicator, value, country, category, frequency, method, unit, description, processed
                                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    ON CONFLICT (date, source, code, country, frequency) DO NOTHING
                                    """,
                            rows,
                        )
                        logger.info("Data loaded successfully.")

                    except psycopg.errors.UndefinedTable:
                        # create table if not exists and retry insert
                        logger.warning(
                            "Table staging_data does not exist, creating table..."
                        )
                        await self.create_table()

                        logger.info("Table staging_data created, retrying insert...")
                        # retry insert with new connection
                        async with self.pool.connection() as retry_aconn:
                            async with retry_aconn:
                                async with retry_aconn.cursor() as retry_acur:
                                    await retry_acur.executemany(
                                        """
                                                INSERT INTO staging_data (
                                                    date, year, source, code, indicator, value, country, category, frequency, method, unit, description, processed
                                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                                ON CONFLICT (date, source, code, country, frequency) DO NOTHING
                                                """,
                                        rows,
                                    )
                                    logger.info(
                                        "Data loaded successfully after creating table."
                                    )
                    except psycopg_pool.PoolTimeout:
                        logger.error(
                            "Connection pool timeout while trying to load data."
                        )
                        SystemExit(1)
                    except psycopg_pool.PoolClosed as e:
                        logger.error(
                            "Connection pool is closed while trying to load data: %s", e
                        )
                        SystemExit(1)
                    except psycopg.OperationalError as e:
                        logger.error(
                            "Operational error while trying to load data: %s", e
                        )
                        SystemExit(1)
