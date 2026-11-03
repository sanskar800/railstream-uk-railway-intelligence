# RailStream

Real-time GB rail performance lakehouse: live train-movement events are streamed through Kafka (Redpanda) and Spark Structured Streaming into Apache Iceberg, then modelled with dbt into punctuality, delay-hotspot and reactionary-delay metrics.

> **Status:** early development. Results will be published here only from measured runs.

## Data and licensing

Uses Network Rail open data (TRUST train movements, CORPUS location reference data) under the Open Government Licence. **This is an unofficial, independent project.** It is not affiliated with or endorsed by Network Rail.

## Development

See [docs/development.md](docs/development.md) for setup.
