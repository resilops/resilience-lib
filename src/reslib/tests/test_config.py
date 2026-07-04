from reslib.config import ResilienceLibConfig


def test_resilience_lib_config_reads_prefixed_environment(monkeypatch):
    monkeypatch.setenv("RESILOPS_RESILIENCE_LIB_IN_CLUSTER_CONFIG", "false")
    monkeypatch.setenv(
        "RESILOPS_RESILIENCE_LIB_POD_TERMINATION_DEFAULT_GRACE_PERIOD", "45"
    )

    settings = ResilienceLibConfig()

    assert settings.in_cluster_config is False
    assert settings.pod_termination_default_grace_period == 45


def test_resilience_lib_config_defaults_are_stable():
    settings = ResilienceLibConfig(
        _env_file=None,
        _env_prefix="UNUSED_PREFIX_",
    )

    assert settings.in_cluster_config is True
    assert settings.pod_termination_default_grace_period == 30
