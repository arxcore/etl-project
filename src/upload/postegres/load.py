import psycopg
from config.settings import Resources
from pipeline.processors.indicator import StagingData
import monitoring.exc_models as exc


class LoadDatabase:
    def __init__(self, resource: Resources) -> None:
        self.resource = resource.postgres_dsn

    async def load_to_db(self, data: StagingData):
        """Load Processed Data to Postgres Database"""
        # FIX: This is a simple implementation for loading data to Postgres, it can be improved by using connection pooling and handling exceptions accordingly.
        # Also, it can be refactored to use an ORM like SQLAlchemy for better maintainability and scalability.
        # ConectionPool management can be implemented using psycopg's connection pool or using an external library like SQLAlchemy's connection pool.
        # This will help to manage database connections efficiently and improve performance when handling multiple requests.
        # make DDL run only once, by checking if the table exists before creating it.
        # This can be done by querying the information_schema or using a flag in the application to track if the table has been created.

        # validate resource not None
        if not self.resource:
            raise exc.ResourceNotFound("Resource postgres dsn not found")

        # create  AsyncConnection
        aconn = await psycopg.AsyncConnection.connect(self.resource)
        async with aconn:
            async with aconn.cursor() as cur:
                try:
                    # Create table if not exists
                    await cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS raw_data (
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
                    # Insert data into table
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
                    # Use executemany to insert multiple rows at once
                    await cur.executemany(
                        """
                        INSERT INTO raw_data (
                            date, year, source, code, indicator, value, country, category, frequency, method, unit, description, processed
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (date, source, code, country, frequency) DO NOTHING
                        """,
                        rows,
                    )
                except Exception as e:
                    await aconn.rollback()
                    raise exc.UploadFailed("Error loading data to Postgres", e) from e
                else:
                    await aconn.commit()
