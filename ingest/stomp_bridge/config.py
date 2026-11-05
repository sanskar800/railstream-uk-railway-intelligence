"""Bridge settings, read from environment variables or `.env` (prefix `NR_`)."""

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class BridgeSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="NR_", extra="ignore")

    username: str
    password: SecretStr  # never printed in logs or reprs
    stomp_host: str = "publicdatafeeds.networkrail.co.uk"
    stomp_port: int = 61618
    topic: str = "TRAIN_MVT_ALL_TOC"
    # Durable subscription name: unique per feed (Open Rail Data wiki, "Durable Subscription").
    subscription_name: str = "railstream-trust"
    heartbeat_ms: int = 15_000
