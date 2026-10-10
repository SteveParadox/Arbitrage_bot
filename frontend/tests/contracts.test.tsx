import { afterEach, describe, expect, it } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { TradingState } from "../src/TradingState";
import { validateControl, validateHealth } from "../src/contracts";

afterEach(cleanup);

describe("control safety contracts", () => {
  it("rejects an old response and missing health safety fields", () => {
    expect(() => validateControl({ status: "stopped", effective_enabled: false })).toThrow();
    expect(() => validateHealth({ status: "ok", trading: { effective_enabled: true } })).toThrow();
  });
  it("accepts and visibly displays uncertainty", () => {
    validateControl({ status: "stop_requested_fallback", effective_enabled: null, engine_state_confirmed: false,
      stop_outcome: "STOP_UNCONFIRMED", request_id: "test", exposure_confirmed_flat: false });
    render(<TradingState enabled={false} outcome="STOP_UNCONFIRMED" />);
    expect(screen.getByRole("status").textContent).toBe("Stop unconfirmed");
  });
  it("shows unknown when the last state is stale, including a previous enabled state", () => {
    render(<TradingState enabled={true} outcome="CONFIRMED_STOPPED" stale />);
    expect(screen.getByRole("status").textContent).toBe("Unknown");
  });
  it("never calls disabled trading proof of closed exposure", () => {
    render(<TradingState enabled={false} outcome="CONFIRMED_STOPPED" />);
    expect(screen.getByRole("status").textContent).toBe("Disabled");
  });
});

it("rejects a stopped label paired with an unconfirmed engine", () => {
  expect(() => validateControl({ status: "stopped", effective_enabled: false, engine_state_confirmed: false,
    stop_outcome: "STOP_UNCONFIRMED", request_id: "test", exposure_confirmed_flat: false })).toThrow();
});
