"use strict";
const $=id=>document.getElementById(id);
const text=(node,value)=>{const v=String(value ?? "—");if(node.textContent!==v)node.textContent=v;};
const stateNames={STOPPED:"已停止",STARTING:"连接中",RUNNING:"运行中",STOPPING:"停止中",ERROR:"异常",SAFE_MODE:"安全模式",WAITING:"等待条件",PAUSED:"已暂停",EXECUTING:"挂单 / 对冲中",CANCELING:"撤单确认中",OPEN:"已开仓",CLOSING:"平仓中",DONE:"本轮结束",CANCELED:"已取消",FAILED:"失败",REVIEW:"待核对"};
const reasons={quote_missing:"缺少双边报价",quote_stale:"报价过期",quote_future:"报价时间超前",quote_skew:"双边报价时间差超限",session_inactive:"会话未运行"};
const quoteAge=(quote,now)=>quote?Math.max(now-quote.exchange_ts_ms,now-quote.local_ts_ms):null;
function validityText(s,c){
 if(s.quotes_valid)return "可触发条件";
 if(s.quote_reason==="quote_stale"){
  const stale=[];for(const [venue,label] of [["binance","Binance"],["mt5","MT5"]]){const age=quoteAge(s[venue],s.timestamp_ms);if(age>c.max_quote_age_ms)stale.push(label+" "+age+" ms 未更新");}
  return "禁止触发 · "+(stale.join("；")||"报价过期");
 }
 if(s.quote_reason==="quote_skew"&&s.binance&&s.mt5)return "禁止触发 · 双边时间差 "+Math.abs(s.binance.exchange_ts_ms-s.mt5.exchange_ts_ms)+" ms";
 return "禁止触发 · "+(reasons[s.quote_reason]||"行情不可用");
}
let current=null,config=null,token="",busy=false,online=false,initialized=false,draft=null,timer=null;
const rows=new Map();
async function request(url,body){
 const r=await fetch(url,{cache:"no-store",method:body===undefined?"GET":"POST",headers:body===undefined?{}:{"X-Control-Token":token,"Content-Type":"application/json"},body:body===undefined?undefined:JSON.stringify(body),signal:AbortSignal.timeout(body===undefined?4000:15000)});
 const data=await r.json();if(!r.ok)throw Error(data.error || "HTTP "+r.status);return data;
}
function controls(){
 const running=online&&!busy&&current?.status==="RUNNING",safe=current?.state==="SAFE_MODE";
 $("start").disabled=!online||busy||["STARTING","RUNNING","STOPPING"].includes(current?.status);
 $("stop").disabled=!online||busy||!["STARTING","RUNNING"].includes(current?.status);
 $("create").disabled=!running||safe;$("close-all").hidden=current?.mode!=="live";
 $("close-all").disabled=!running||safe||!current?.conditions.some(c=>c.state==="OPEN");
 document.querySelectorAll("[data-action]").forEach(b=>b.disabled=!running||(b.dataset.action==="close"&&safe));
}
async function command(path,body={}){
 if(busy)return false;busy=true;controls();
 try{await request(path,body);text($("error"),"");return true;}
 catch(e){text($("error"),"操作未确认："+e.message+"。请核对列表，创建重试会保留同一请求编号。");return false;}
 finally{busy=false;controls();schedule(0);}
}
$("start").onclick=()=>command("/api/start");$("stop").onclick=()=>command("/api/stop");
$("close-all").onclick=()=>command("/api/close-all");
$("form").onsubmit=async e=>{
 e.preventDefault();if($("create").disabled)return;
 const fields={direction:$("direction").value,entry_threshold:$("entry").value,cancel_threshold:$("cancel").value,quantity:$("quantity").value,repeat:$("repeat").checked,exit_threshold:current.mode==="live"?$("exit").value||null:null,min_net_profit:current.mode==="live"?$("net-profit").value||null:null};
 const signature=JSON.stringify(fields);if(draft?.signature!==signature)draft={signature,id:crypto.randomUUID()};
 if(await command("/api/conditions",{...fields,request_id:draft.id}))draft=null;
};
function copy(c){$("direction").value=c.direction;$("entry").value=c.entry_threshold;$("cancel").value=c.cancel_threshold;$("quantity").value=c.quantity;$("repeat").checked=c.repeat;$("exit").value=c.exit_threshold??"";$("net-profit").value=c.min_net_profit??"";draft=null;$("entry").focus();}
function render(s){
 if(s.config)config=s.config;if(!config)throw Error("状态响应缺少配置");
 const c=config;current={...s,config};token=s.control_token;online=true;
 text($("pair"),c.symbols.binance+" / "+c.symbols.mt5);text($("mode"),s.mode==="live"?"LIVE 实盘":"PAPER 本地模拟");
 $("mode").className=s.mode==="live"?"live":"";text($("session"),stateNames[s.status]||s.status);
 text($("connection"),"已连接 · "+new Date(s.timestamp_ms).toLocaleTimeString("zh-CN"));
 text($("validity"),validityText(s,c));$("validity").className=s.quotes_valid?"good":"bad";
 for(const venue of ["binance","mt5"]){
  const q=s[venue];text($(venue+"-symbol"),c.symbols[venue]);text($(venue+"-bid"),q?.bid);text($(venue+"-ask"),q?.ask);
  const delay=q?q.local_ts_ms-q.exchange_ts_ms:null;
  text($(venue+"-latency"),!s.connections[venue]?"未连接":delay<0?delay+" ms · 时钟差":delay+" ms");
 }
 text($("a"),s.directions.SHORT_BINANCE.raw_spread);text($("b"),s.directions.LONG_BINANCE.raw_spread);
 text($("slot"),s.active_condition_id?"执行中："+s.active_condition_id.slice(0,8)+" · 其余单等待":"执行槽空闲");
 text($("risk"),s.error||"");
 if(!initialized){$("entry").value=c.entry_threshold;$("cancel").value=c.cancel_threshold;$("quantity").value=c.binance_qty;initialized=true;}
 $("exit").disabled=s.mode!=="live";$("net-profit").disabled=s.mode!=="live";
 const visible=s.conditions.filter(c=>$("finished").checked||!["DONE","CANCELED","FAILED"].includes(c.state)),ids=new Set(visible.map(c=>c.request_id));
 for(const [id,row] of rows)if(!ids.has(id)){row.remove();rows.delete(id);}
 for(const [index,c] of visible.entries()){
  let row=rows.get(c.request_id);
  if(!row){row=document.createElement("tr");for(let i=0;i<11;i++)row.insertCell();row.dataset.id=c.request_id;rows.set(c.request_id,row);}
  row.condition=c;
  const values=[c.request_id.slice(0,8)+" / "+c.queue_seq,c.direction==="SHORT_BINANCE"?"A 空 Binance / 多 MT5":"B 多 Binance / 空 MT5",(c.raw_spread??"—")+" / "+(c.edge??"—"),c.entry_threshold,c.cancel_threshold,(c.exit_threshold??"—")+" / 净≥ "+(c.min_net_profit??"—"),c.quantity,c.repeat?"循环":"单次",(stateNames[c.state]||c.state)+(c.close_requested?" · 平仓排队":"")+(c.state==="WAITING"?" · "+c.confirmation.count+"/"+config.min_ticks+" Tick · "+c.confirmation.duration_ms+"/"+config.min_duration_ms+" ms":"")];
  values.push("实入 "+(c.actual_entry_spread??"—")+" / 实出 "+(c.actual_exit_spread??"—")+" / 报价估算收益点差 "+(c.estimated_spread_gain??"—")+" / 预计净 "+(c.profit_estimate?.expected_net_pnl??"未知"));
  values.forEach((v,i)=>text(row.cells[i],v));
  const actions=c.state==="WAITING"?["pause","cancel"]:c.state==="PAUSED"?["resume","cancel"]:c.state==="OPEN"?["close"]:["EXECUTING","CANCELING"].includes(c.state)?["cancel"]:[];
  const signature=actions.join();
  if(row.actions!==signature){row.actions=signature;row.cells[10].replaceChildren();for(const action of [...actions,"copy","detail"]){const b=document.createElement("button");text(b,({pause:"暂停",resume:"恢复",cancel:"取消",close:"平仓",copy:"复制",detail:"详情"})[action]);if(!["copy","detail"].includes(action))b.dataset.action=action;b.onclick=()=>{const v=row.condition;if(action==="copy")copy(v);else if(action==="detail"){text($("detail"),JSON.stringify(v,null,2));$("detail-box").open=true;}else command("/api/conditions/"+encodeURIComponent(v.request_id)+"/"+action);};row.cells[10].append(b);}}
  const at=$("conditions").children[index];if(at!==row)$("conditions").insertBefore(row,at||null);
 }
 $("empty").hidden=visible.length>0;
 if($("diagnostics").open){
  const lines=["接收延迟 = 本机收到时间 − 交易端报价时间；受两端时钟同步和交易端时间戳精度影响。","年龄上限 "+c.max_quote_age_ms+" ms；双边时间差上限 "+c.max_quote_skew_ms+" ms","连续确认 "+c.min_ticks+" Tick + "+c.min_duration_ms+" ms；挂单等待上限 "+c.max_pending_ms+" ms"];
  for(const v of ["binance","mt5"]){const q=s[v];if(q)lines.push(v+" 接收延迟 "+(q.local_ts_ms-q.exchange_ts_ms)+" ms；距最后更新 "+quoteAge(q,s.timestamp_ms)+" ms");}
  text($("diagnostic-text"),lines.join("\n"));
 }
 controls();
}
$("finished").onchange=()=>{if(current)render(current);};
$("diagnostics").ontoggle=()=>{if(current)render(current);};
$("accounts").ontoggle=async()=>{if(!$("accounts").open)return;text($("account-text"),"加载中");try{const s=await request("/api/status");text($("account-text"),JSON.stringify({accounts:s.accounts,error:s.accounts_error,updated_at:s.accounts_updated_ms},null,2));}catch(e){text($("account-text"),e.message);}};
$("refresh-records").onclick=async()=>{
 const b=$("refresh-records");b.disabled=true;
 try{const [s,h]=await Promise.all([request("/api/status"),request("/api/history")]);text($("records"),JSON.stringify({settlement_totals:s.settlement_totals,note:"已结算尝试包含失败补偿；按币种统计，未分摊 Binance 资金费；未结算项不计作零收益",trades:s.trades,orders:h.orders,events:s.events},null,2));}
 catch(e){text($("records"),e.message);}finally{b.disabled=false;}
};
let polling=false;
function schedule(delay){clearTimeout(timer);timer=setTimeout(poll,delay);}
async function poll(){
 if(polling){schedule(100);return;}polling=true;
 try{render(await request("/api/status?compact=1"+(config?"":"&config=1")));}
 catch(e){online=false;text($("connection"),"离线 · 显示值已冻结");text($("validity"),"连接中断，无法判断行情有效性");$("validity").className="bad";text($("risk"),e.message);for(const v of ["binance","mt5"])text($(v+"-latency"),"未知 / 离线");controls();}
 finally{polling=false;schedule(document.hidden?5000:500);}
}
document.addEventListener("visibilitychange",()=>schedule(0));
poll();
