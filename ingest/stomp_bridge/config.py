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
    # Durable subscription = client-id + subscription name (Open Rail Data wiki). Two bridges with
    # the SAME pair share one subscription and split the messages (findings F6); a different pair
    # is a separate subscription. client-id defaults to the account email.
    client_id: str | None = None
    subscription_name: str = "railstream-trust"
    heartbeat_ms: int = 15_000
    # No frame for this long while connected = "feed quiet" (a full day's longest gap was 20 s).
    quiet_after_s: int = 60

    @property
    def effective_client_id(self) -> str:
        return self.client_id or self.username
