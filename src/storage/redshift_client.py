from __future__ import annotations

import logging
from typing import Optional

import psycopg2
from psycopg2.extensions import connection as PGConnection

from src.common.settings import Settings


logger = logging.getLogger(__name__)


class RedshiftClient:
    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
    ):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

    @classmethod
    def from_settings(cls, settings: Settings) -> "RedshiftClient":
        return cls(
            host=settings.redshift.host,
            port=int(settings.redshift.port),
            database=settings.redshift.database,
            user=settings.redshift.user,
            password=settings.redshift.password,
        )

    def _connect(self) -> PGConnection:
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.database,
            user=self.user,
            password=self.password,
        )

    def execute(self, sql: str) -> None:
        """
        Execute arbitrary SQL (DDL / DML).
        """
        with self._connect() as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(sql)

    def copy_parquet_from_s3(
        self,
        s3_bucket: str,
        s3_key: str,
        schema: str,
        table: str,
        region: str,
        iam_role: Optional[str] = None,
    ) -> None:
        """
        COPY parquet file from S3 into Redshift table.
        """
        if iam_role is None:
            # Expect IAM role attached to cluster
            cred_clause = ""
        else:
            cred_clause = f"IAM_ROLE '{iam_role}'"

        sql = f"""
        COPY {schema}.{table}
        FROM 's3://{s3_bucket}/{s3_key}'
        FORMAT AS PARQUET
        REGION '{region}'
        {cred_clause};
        """

        logger.info("COPY into %s.%s from s3://%s/%s", schema, table, s3_bucket, s3_key)
        self.execute(sql)
