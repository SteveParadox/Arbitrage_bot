import {afterEach,expect,it,vi} from "vitest";
import {cleanup,fireEvent,render,screen,waitFor} from "@testing-library/react";
import {ExecutionPanel} from "../src/ExecutionPanel";
import {validateExecutionStatus,validateExecutionCycles,validateExecutionOrders} from "../src/executionContracts";
const status={account_id:"account",mode:"testnet",environment:"offline_mock",state:"READY",authorized:false,blocked:false,recovering:false,emergency_stop:false,runtime_online:true,can_control:true,mock_evidence:true,exchange_confirmed:false,market_ready:true,active_cycles:0,snapshot_ms:1,committed_usdt:"0",balances:{wallet:{USDT:"1000.00000001"},available:{USDT:"1000.00000001"},locked:{USDT:"0"}}};
afterEach(()=>{cleanup();vi.unstubAllGlobals();});
it("rejects mixed modes, invented lifecycle and numeric balances",()=>{
  expect(validateExecutionStatus(status).balances.wallet?.USDT).toBe("1000.00000001");
  for(const patch of [{mode:"paper"},{mode:"live"},{exchange_confirmed:true},{state:"PROFITABLE"},{can_control:undefined},{balances:{wallet:{USDT:1000}}}])expect(()=>validateExecutionStatus({...status,...patch})).toThrow();
});
it.each(["testnet","live"])("requires explicit authorization before %s start",async mode=>{
  const s=mode==="live"?{...status,mode,environment:"live",mock_evidence:false,exchange_confirmed:true}:status;
  vi.stubGlobal("fetch",vi.fn(async(url:string)=>({ok:true,json:async()=>url.endsWith("status")?s:[]})));
  render(<ExecutionPanel/>);
  fireEvent.change(screen.getByLabelText("Exchange account"),{target:{value:"account"}});
  fireEvent.change(screen.getByLabelText("Execution credential"),{target:{value:"operator-token"}});
  fireEvent.click(screen.getByText("Connect exchange account"));
  await waitFor(()=>expect(screen.queryByText(`Start ${mode} execution`)).toBeTruthy());
  const button=screen.getByText(`Start ${mode} execution`) as HTMLButtonElement;
  expect(button.disabled).toBe(true);
  fireEvent.change(screen.getByLabelText("Execution authorization"),{target:{value:mode==="live"?"AUTHORIZE MAINNET":"AUTHORIZE TESTNET"}});
  expect(button.disabled).toBe(false);
  expect(vi.mocked(fetch).mock.calls.every(c=>!(c[1] as RequestInit)?.method)).toBe(true);
});
it("keeps recovery inventory and unverified P&L distinct from a completed cycle",async()=>{
  vi.stubGlobal("fetch",vi.fn(async(url:string)=>({ok:true,json:async()=>url.endsWith("status")?{...status,blocked:true,state:"BLOCKED"}:url.endsWith("cycles")?[{id:"unknown",state:"RECOVERY_REQUIRED",starting:"100",realized_pnl:null,holdings:{BTC:"0.99"}}]:[]})));
  render(<ExecutionPanel/>);
  fireEvent.change(screen.getByLabelText("Exchange account"),{target:{value:"account"}});
  fireEvent.change(screen.getByLabelText("Execution credential"),{target:{value:"token"}});
  fireEvent.click(screen.getByText("Connect exchange account"));
  await waitFor(()=>expect(screen.queryByText("Unresolved financial outcome")).toBeTruthy());
  expect(screen.getByText("0.99 BTC")).toBeTruthy();
  expect((screen.getByText("Start testnet execution") as HTMLButtonElement).disabled).toBe(true);
});
it("rejects malformed and duplicate financial records before rendering",()=>{
  const cycle={id:"cycle",state:"RECOVERY_REQUIRED",starting:"100",realized_pnl:null,holdings:{BTC:"0.99"}};
  expect(validateExecutionCycles([cycle])).toHaveLength(1);
  for(const value of [[{...cycle,holdings:null}],[{...cycle,realized_pnl:5}],[cycle,cycle]])expect(()=>validateExecutionCycles(value)).toThrow();
  const order={link_id:"stable",state:"SUBMISSION_UNKNOWN",intent:{symbol:"BTCUSDT",side:"Buy"},evidence:null};
  expect(validateExecutionOrders([order])).toHaveLength(1);
  for(const value of [[{...order,intent:null}],[{...order,evidence:{fees:{USDT:1}}}],[order,order]])expect(()=>validateExecutionOrders(value)).toThrow();
  expect(validateExecutionStatus({...status,environment:"testnet",mock_evidence:false,exchange_confirmed:false})).toBeTruthy();
});
