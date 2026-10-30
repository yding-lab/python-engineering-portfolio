"""Database connectivity and transaction utilities.

This module centralizes PostgreSQL connection creation, health checks,
transaction handling, and table metadata used by the loading layer.

Credentials come only from the environment (DB_USER, DB_PASSWORD). There are no defaults:
a missing credential is a configuration error, never a silent fallback to postgres/postgres.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Generator

from sqlalchemy import Engine, create_engine, text
from sqlalchemy.engine import URL, Connection
from sqlalchemy.exc import SQLAlchemyError


class DatabaseConnectionError(RuntimeError):
    """Raised when the application cannot connect to PostgreSQL."""


class DatabaseConfigError(ValueError):
    """Raised when required database settings are missing."""


@dataclass(frozen=True)
class DatabaseConfig:
    """Connection settings for the PostgreSQL warehouse."""

    username: str
    password: str = field(repr=False)  # never printed, even in a debugger or a log of the config
    host: str = "localhost"
    port: int = 5432
    database: str = "portfolio"
    connect_timeout: int = 10

    @classmethod
    def from_env(cls) -> "DatabaseConfig":
        """Build database configuration from environment variables (see .env.example)."""
        missing = [name for name in ("DB_USER", "DB_PASSWORD") if not os.getenv(name)]
        if missing:
            raise DatabaseConfigError(
                f"Missing required environment variable(s): {', '.join(missing)}. "
                "Copy .env.example to .env, fill it in, and export it (see README)."
            )
        return cls(
            username=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "5432")),
            database=os.getenv("DB_NAME", "portfolio"),
            connect_timeout=int(os.getenv("DB_CONNECT_TIMEOUT", "10")),
        )

    @property
    def sqlalchemy_url(self) -> URL:
        """SQLAlchemy URL object. Special characters in the password are escaped, and
        str(url) masks the password as ***."""
        return URL.create(
            "postgresql+psycopg2",
            username=self.username,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
        )

    def safe_url(self) -> str:
        """Connection string safe to log."""
        return self.sqlalchemy_url.render_as_string(hide_password=True)


def create_database_engine(
    config: DatabaseConfig | None = None,
    *,
    echo: bool = False,
) -> Engine:
    """Create a production-oriented SQLAlchemy engine.

    pool_pre_ping protects the application from stale pooled connections.
    pool_recycle avoids very long-lived connections in development or cloud
    environments.
    """
    config = config or DatabaseConfig.from_env()

    try:
        return create_engine(
            config.sqlalchemy_url,
            echo=echo,
            pool_pre_ping=True,
            pool_recycle=1800,
            connect_args={"connect_timeout": config.connect_timeout},
        )
    except SQLAlchemyError as exc:
        raise DatabaseConnectionError(
            f"Unable to create database engine: {exc}"
        ) from exc


def check_database_connection(engine: Engine) -> bool:
    """Return True when PostgreSQL responds to a lightweight health check."""
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            return result.scalar_one() == 1
    except SQLAlchemyError as exc:
        raise DatabaseConnectionError(
            f"Database health check failed: {exc}"
        ) from exc


@contextmanager
def transactional_connection(
    engine: Engine,
) -> Generator[Connection, None, None]:
    """Provide a transaction that automatically commits or rolls back.

    Any exception raised inside the context causes a rollback.
    """
    connection = engine.connect()
    transaction = connection.begin()

    try:
        yield connection
        transaction.commit()
    except Exception:
        transaction.rollback()
        raise
    finally:
        connection.close()


def ensure_indicator_table(engine: Engine) -> None:
    """Create the warehouse table and indexes if they do not exist."""
    ddl = """
    CREATE TABLE IF NOT EXISTS economic_indicators (
        record_key VARCHAR(150) PRIMARY KEY,
        country_code VARCHAR(3) NOT NULL,
        country_name VARCHAR(120),
        year INTEGER NOT NULL,
        indicator_code VARCHAR(50) NOT NULL,
        indicator_name VARCHAR(255),
        value DOUBLE PRECISION NOT NULL,
        year_over_year_pct_change DOUBLE PRECISION,
        source_updated_at TIMESTAMP,
        pipeline_loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_economic_indicators_country_year
        ON economic_indicators (country_code, year);

    CREATE INDEX IF NOT EXISTS idx_economic_indicators_indicator_year
        ON economic_indicators (indicator_code, year);
    """

    try:
        with engine.begin() as connection:
            for statement in [s.strip() for s in ddl.split(";") if s.strip()]:
                connection.execute(text(statement))
    except SQLAlchemyError as exc:
        raise DatabaseConnectionError(
            f"Unable to initialize warehouse schema: {exc}"
        ) from exc
