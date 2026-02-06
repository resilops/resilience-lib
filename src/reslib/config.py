from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ResilienceLibConfig(BaseSettings):

    model_config = SettingsConfigDict(env_prefix="RESILTY_RESILIENCE_LIB_")

    in_cluster_config: bool = Field(
        default=True, description="If its true, k8 will load config within the pod"
    )

    pod_termination_default_grace_period: int = Field(
        default=30, ge=0, description="Default pod termination grace period (seconds)."
    )


config: ResilienceLibConfig = ResilienceLibConfig()
