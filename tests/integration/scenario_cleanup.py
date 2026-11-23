"""Remove what an integration scenario created, after it passed (in the Spark container).

A passing run leaves nothing behind in the shared catalog, Kafka or the checkpoint volume. A
failing run never reaches this, so its tables stay for inspection. (Earlier versions kept every
run's tables, and test namespaces piled up next to the real ones in the catalog.)
"""

import shutil


def remove_scenario(spark, namespace: str, topic: str | None, checkpoint: str | None) -> None:
    for row in spark.sql(f"SHOW TABLES IN {namespace}").collect():
        spark.sql(f"DROP TABLE {namespace}.{row.tableName}")  # plain DROP: Lakekeeper deletes files
    spark.sql(f"DROP NAMESPACE IF EXISTS {namespace}")
    if topic:
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": "redpanda:9092"})
        admin.delete_topics([topic])[topic].result()
    if checkpoint:
        shutil.rmtree(checkpoint, ignore_errors=True)
