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
    pod_termination_max_timeout: int = Field(
        default=300,
        gt=30,
        description="Maximum time to wait for pod termination (seconds).",
    )


config: ResilienceLibConfig = ResilienceLibConfig()
