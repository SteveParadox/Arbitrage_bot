"""Run from the repository root after modifying api/contracts.py."""
import json
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root / "python"))
from api.contracts import HealthResponse, TradingControlResponse

for name, model in [("health-response", HealthResponse), ("trading-control-response", TradingControlResponse)]:
    (root / "shared" / "schemas" / f"{name}.schema.json").write_text(
        json.dumps(model.model_json_schema(mode="serialization"), indent=2) + "\n")
