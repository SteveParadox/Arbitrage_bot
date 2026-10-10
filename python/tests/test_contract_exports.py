import json
import yaml
from pathlib import Path
from api.contracts import HealthResponse, TradingControlResponse


def test_shared_frontend_contracts_match_backend():
    root = Path(__file__).resolve().parents[2]
    for name, model in [
        ("health-response", HealthResponse),
        ("trading-control-response", TradingControlResponse),
    ]:
        saved = json.loads((root / "shared" / "schemas" / f"{name}.schema.json").read_text())
        assert saved == model.model_json_schema(mode="serialization")


def test_development_postgres_is_loopback_and_api_waits_for_migrations():
    class UniqueKeysLoader(yaml.SafeLoader):
        pass

    def unique_mapping(loader, node):
        values = {}
        for key_node, value_node in node.value:
            key = loader.construct_object(key_node)
            assert key not in values, f"duplicate Compose key: {key}"
            values[key] = loader.construct_object(value_node)
        return values

    UniqueKeysLoader.add_constructor(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, unique_mapping
    )
    compose = yaml.load(
        (Path(__file__).resolve().parents[2] / "docker/docker-compose.yml").read_text(),
        Loader=UniqueKeysLoader,
    )
    services = compose["services"]
    assert services["postgres"]["ports"] == ["127.0.0.1:5432:5432"]
    assert services["migrate"]["command"] == ["alembic", "upgrade", "head"]
    assert services["api"]["depends_on"] == {
        "migrate": {"condition": "service_completed_successfully"},
        "redis": {"condition": "service_healthy"},
        "engine-control": {"condition": "service_started"},
    }
    assert services["engine-control"]["build"]["dockerfile"] == "docker/Dockerfile.engine"
