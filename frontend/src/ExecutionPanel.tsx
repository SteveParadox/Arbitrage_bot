import { useEffect, useState } from "react";
import { API_URL } from "./api";
import { validateExecutionStatus, validateExecutionCycles, validateExecutionOrders, type ExecutionStatus, type ExecutionCycle, type ExecutionOrder } from "./executionContracts";
export function ExecutionPanel() {
  const [account,setAccount]=useState(""); const [token,setToken]=useState("");
  const [connected,setConnected]=useState(false); const [status,setStatus]=useState<ExecutionStatus|null>(null);
  const [cycles,setCycles]=useState<ExecutionCycle[]>([]); const [orders,setOrders]=useState<ExecutionOrder[]>([]);
  const [error,setError]=useState(""); const [confirmation,setConfirmation]=useState(""); const [busy,setBusy]=useState(false);
  useEffect(()=>{
    if (!connected) return;
    let active=true; const controller=new AbortController();
    const refresh=async()=>{
      try {
        const responses=await Promise.all(["status","cycles","orders"].map(entity=>fetch(`${API_URL}/execution/${encodeURIComponent(account)}/${entity}`,{headers:{Authorization:`Bearer ${token}`},signal:controller.signal})));
        if (responses.some(r=>!r.ok)) throw new Error(`Execution refresh failed (${responses.find(r=>!r.ok)?.status})`);
        const [s,c,o]=await Promise.all(responses.map(r=>r.json()));
        const validated=validateExecutionStatus(s);
        const cycleRows=validateExecutionCycles(c);const orderRows=validateExecutionOrders(o);
        if (active) {setStatus(validated);setCycles(cycleRows);setOrders(orderRows);setError("");}
      } catch(e) {if(active){setStatus(null);setError(String(e));}}
    };
    void refresh();const interval=setInterval(()=>void refresh(),2000);
    return()=>{active=false;controller.abort();clearInterval(interval);};
  },[connected,account,token]);
  async function command(action:"start"|"stop"|"emergency-stop") {
    if(!status||busy||!status.can_control)return;
    setBusy(true);
    try {
      const response=await fetch(`${API_URL}/execution/${encodeURIComponent(account)}/control/${action}`,{method:"POST",headers:{Authorization:`Bearer ${token}`,"Content-Type":"application/json"},body:JSON.stringify({request_id:crypto.randomUUID(),reason:`Explicit operator ${action}`,authorization:action==="start"?(status.mode==="live"?"authorize_mainnet":"authorize_testnet"):"stop"})});
      if(!response.ok) throw new Error(`Execution control failed (${response.status})`);
      setConfirmation("");setStatus(null);
    } catch(e){setError(String(e));} finally {setBusy(false);}
  }
  const phrase=status?.mode==="live"?"AUTHORIZE MAINNET":"AUTHORIZE TESTNET";
  const canStart=status?.can_control===true&&status.state==="READY"&&status.active_cycles===0&&status.runtime_online&&status.market_ready&&!status.blocked&&!status.recovering&&!status.emergency_stop&&!status.authorized&&confirmation===phrase;
  return <section className="section-card"><div className="section-heading"><div><h2>Authenticated execution</h2><p>Separate exchange accounts, confirmed fill evidence and reconciliation. Paper results remain in the paper panel.</p></div></div>
    <div className="paper-controls"><label>Exchange account <input aria-label="Exchange account" value={account} disabled={connected} onChange={e=>setAccount(e.target.value)}/></label>
      <label>Execution credential <input aria-label="Execution credential" type="password" autoComplete="off" value={token} disabled={connected} onChange={e=>setToken(e.target.value)}/></label>
      <button disabled={!connected&&(!account||!token)} onClick={()=>{setConnected(!connected);setStatus(null);setCycles([]);setOrders([]);setConfirmation("");}}>{connected?"Disconnect execution":"Connect exchange account"}</button></div>
    {error&&<p role="alert">{error}</p>}
    {status&&<><p><strong>{status.mode.toUpperCase()} · {status.environment==="offline_mock"?"OFFLINE MOCK · No exchange confirmation":status.mode==="live"?"MAINNET":"TESTNET"}</strong> · {status.state} · {status.authorized?"Authorized":"Disabled"} · {status.active_cycles} active cycles</p>
      <p>Last wallet evidence: {status.snapshot_ms===null?"Unavailable":new Date(status.snapshot_ms).toISOString()} · Committed capital {status.committed_usdt} USDT · Emergency stop {status.emergency_stop?"latched":"clear"}</p>
      <div className="paper-controls"><label>Explicit execution authorization <input aria-label="Execution authorization" placeholder={phrase} value={confirmation} onChange={e=>setConfirmation(e.target.value)}/></label>
        <button disabled={!canStart||busy} onClick={()=>void command("start")}>Start {status.mode} execution</button>
        <button disabled={!status.can_control||busy} onClick={()=>void command("stop")}>Stop new exposure</button>
        <button disabled={!status.can_control||busy} onClick={()=>void command("emergency-stop")}>Emergency stop execution</button></div>
      <p>Stop changes durable authorization. Active orders and inventory require confirmed reconciliation; a successful command does not confirm exchange cancellation.</p>
      <div className="table-scroll"><table><thead><tr><th>Asset</th><th>Wallet</th><th>Exchange available before local commitments</th><th>Exchange locked</th></tr></thead><tbody>{Object.entries(status.balances.wallet??{}).map(([a,v])=><tr key={a}><td>{a}</td><td>{v}</td><td>{status.balances.available?.[a]??"Unavailable"}</td><td>{status.balances.locked?.[a]??"Unavailable"}</td></tr>)}</tbody></table></div>
      <h3>Exchange cycles · P&amp;L in USDT</h3><div className="table-scroll"><table><thead><tr><th>Cycle</th><th>State</th><th>Starting</th><th>Reconciled realized P&amp;L</th><th>Owned holdings</th></tr></thead><tbody>{cycles.map(c=><tr key={c.id}><td>{c.id}</td><td>{c.state}</td><td>{c.starting}</td><td>{c.realized_pnl??"Unresolved financial outcome"}</td><td>{Object.entries(c.holdings).map(([a,v])=>`${v} ${a}`).join(", ")}</td></tr>)}</tbody></table></div>
      <h3>Order evidence</h3><div className="table-scroll"><table><thead><tr><th>Client ID</th><th>Market / side</th><th>State</th><th>Confirmed quantity</th><th>Fees by asset</th></tr></thead><tbody>{orders.map(o=><tr key={o.link_id}><td>{o.link_id}</td><td>{o.intent.symbol} {o.intent.side}</td><td>{o.state}</td><td>{o.evidence?.filled_quantity??"Not confirmed"}</td><td>{Object.entries(o.evidence?.fees??{}).map(([a,v])=>`${v} ${a}`).join(", ")||"Not reported"}</td></tr>)}</tbody></table></div>
    </>}
  </section>;
}
