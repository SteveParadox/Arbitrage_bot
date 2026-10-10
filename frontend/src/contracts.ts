import Ajv from "ajv";
import addFormats from "ajv-formats";
import healthSchema from "../../shared/schemas/health-response.schema.json";
import controlSchema from "../../shared/schemas/trading-control-response.schema.json";
import type { HealthResponse, TradingControlResponse } from "./api";

const ajv = new Ajv({ allErrors: true, strict: false });
addFormats(ajv);
const healthValidator = ajv.compile(healthSchema);
const controlValidator = ajv.compile(controlSchema);

export function validateHealth(value: unknown): asserts value is HealthResponse {
  if (!healthValidator(value)) throw new Error("Invalid health response: safety state is unknown");
  const health = value as HealthResponse;
  if (health.observer.state === "scanning" && health.observer.scanner_ready !== true) {
    throw new Error("Inconsistent observer readiness");
  }
  if (health.trading.effective_enabled && (!health.trading.deployment_enabled || !health.trading.runtime_enabled
      || !health.trading.risk_allows_new_orders || !health.control_auth_configured
      || Object.values(health.trading.dependencies).some((ready) => ready !== true))) {
    throw new Error("Inconsistent health response: trading eligibility is unknown");
  }
}

export function validateControl(value: unknown): asserts value is TradingControlResponse {
  if (!controlValidator(value)) throw new Error("Invalid control response: command outcome is unconfirmed");
  const control = value as TradingControlResponse;
  if ((control.status === "stopped" && (!control.engine_state_confirmed || control.stop_outcome !== "CONFIRMED_STOPPED" || control.effective_enabled !== false))
      || (control.status === "stop_requested_fallback" && (control.engine_state_confirmed || control.stop_outcome !== "STOP_UNCONFIRMED" || control.effective_enabled !== null))) {
    throw new Error("Inconsistent control response: command outcome is unconfirmed");
  }
}
