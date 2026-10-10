import { afterEach, expect, it } from "vitest";
import { cleanup, render, screen } from "@testing-library/react";
import { ObserverState } from "../src/ObserverState";
afterEach(cleanup);
it.each(["degraded", "stale", "unknown", "reconnecting", "failed", "scanning"])("renders %s", (state) => {
  render(<ObserverState state={state} />);
  expect(screen.getByRole("status").textContent).toBe(state);
});
it("expires a scanning display", () => {
  render(<ObserverState state="scanning" stale />);
  expect(screen.getByRole("status").textContent).toBe("stale");
});
it("shows unknown for unsupported telemetry", () => {
  render(<ObserverState state="green" />);
  expect(screen.getByRole("status").textContent).toBe("unknown");
});
