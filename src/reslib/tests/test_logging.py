from unittest.mock import patch

from pythonjsonlogger.json import JsonFormatter

from reslib.logging import setup_logging


def test_setup_logging_uses_reslib_log_level_environment_variable(monkeypatch):
    monkeypatch.setenv("RESLIB_LOG_LEVEL", "DEBUG")

    with patch("logging.config.dictConfig") as dict_config:
        setup_logging()

    config = dict_config.call_args.args[0]
    assert config["loggers"]["reslib"]["level"] == "DEBUG"
    assert config["handlers"]["console"]["formatter"] == "json"
    assert config["handlers"]["console"]["level"] == "DEBUG"

    formatter_config = config["formatters"]["json"]
    assert formatter_config["()"] is JsonFormatter
    assert formatter_config["rename_fields"] == {
        "asctime": "time",
        "levelname": "level",
    }


def test_setup_logging_defaults_reslib_level_to_info(monkeypatch):
    monkeypatch.delenv("RESLIB_LOG_LEVEL", raising=False)

    with patch("logging.config.dictConfig") as dict_config:
        setup_logging()

    config = dict_config.call_args.args[0]
    assert config["loggers"]["reslib"]["level"] == "INFO"
