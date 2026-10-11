import Ajv from "ajv";
import schema from "../../shared/schemas/paper-status.schema.json";
export type PaperStatus = {
  account_id: string; mode: "paper"; simulated: true; state: string;
  clock?: "wall" | "virtual";
  can_control?: boolean;
  authorized: boolean; blocked: boolean; runtime_online: boolean;
  active_cycles: number; pending_orders: number;
  balances: { asset: string; available: string; reserved: string }[];
};
const validate = new Ajv({ strict: false }).compile(schema);
export function validatePaperStatus(value: unknown): PaperStatus {
  if (!validate(value)) throw new Error("Invalid paper status response");
  return value as PaperStatus;
}
