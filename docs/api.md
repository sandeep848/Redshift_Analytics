# API documentation

This project does not expose an HTTP API. Data movement happens through Kafka topics and storage backends (DuckDB, S3, Redshift).

For the streaming contract, see `docs/kafka.md` and `src/common/schema.py`.
