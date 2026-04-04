from datetime import datetime
import psycopg2
import logging
from contextlib import contextmanager
from pipeline.processors.indicator import FinalFormatResult
from config.settings import ResourceAPIkey

logger = logging.getLogger(__name__)


@contextmanager
def connect():
    resource = ResourceAPIkey()
    conn = psycopg2.connect(resource.postgres_dsn)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
    finally:
        conn.close()


def upload_to_db(data: FinalFormatResult):
    logger.info("data to store %s", len(data.format_result))
    # conn
    with connect() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS indicators
                (
                id SERIAL PRIMARY KEY,
                DATE date,
                YEAR integer,
                INDICATOR text,
                COUNTRY text,
                CATEGORY text,
                VALUE float,
                FREQUENCY text,
                PROCESSED timestamp,
                UNIQUE (DATE, INDICATOR, COUNTRY)
                )
                """
            )
            rows = [
                (
                    item.date,
                    item.year,
                    item.indicator,
                    item.country,
                    item.category,
                    item.value,
                    item.frequency,
                    datetime.now().isoformat(),
                )
                for item in data.format_result
            ]
            cur.executemany(
                """
                INSERT INTO indicators (date, year, indicator, country, category, value, frequency, processed)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (date, indicator, country) DO NOTHING                
                """,
                rows,
            )

        except Exception as e:
            raise psycopg2.DatabaseError(f"Unknown Error Into Database : {e}") from e
        finally:
            cur.close()
