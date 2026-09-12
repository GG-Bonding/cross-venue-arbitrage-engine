"use strict";
const $ = id => document.getElementById(id);
const labels = {STOPPED:"已停止",STARTING:"正在连接行情",RUNNING:"监控运行中",STOPPING:"正在停止 · 清理模拟挂单",ERROR:"运行异常",IDLE:"等待信号",CONFIRMING:"连续确认中",CONFIRMED:"已确认",PLACING_MAKER:"创建模拟挂单",MAKER_PENDING:"模拟挂单等待中",CANCELING:"撤单确认中",CANCELED:"已撤单",SAFE_MODE:"安全模式 · 暂停挂单"};
const reasons = {quote_missing:"等待双边行情",quote_stale:"报价已过期",quote_skew:"报价时间差超限",quote_future:"报价时间超前",session_inactive:"引擎未运行"};
const eventNames = {mt5_connected:"MT5 已连接",binance_connected:"Binance 已连接",contract_specifications:"已读取合约规格",paper_reconciliation:"完成本地状态核对",quote_stale:"拒绝过期行情",quote_skew:"拒绝时间差超限行情",quote_future:"拒绝超前时间行情",quote_missing:"等待双边报价",entry_confirmation_start:"开始连续确认",entry_confirmation_reset:"连续确认已复位",entry_confirmed:"入场条件已确认",maker_order_created:"已创建模拟挂单",maker_cancel_requested:"已请求模拟撤单",maker_order_canceled:"模拟撤单已确认",paper_shutdown:"Paper 会话已停止",session_error:"Paper 会话异常"};
let token = null, busy = false, online = false, current = null, lastHistory = 0, sampleTime = 0;
const points = [];
let formInitialized=false, actionError=null, pendingDraft=null;
const conditionLabels={WAITING:"等待条件",EXECUTING:"模拟挂单中",CANCELING:"正在撤单",DONE:"本次结束 · 模拟单已撤",CANCELED:"已取消",PAUSED:"已暂停",REVIEW:"中断待核对",FAILED:"执行失败",OPEN:"已开仓",CLOSING:"双边平仓中"};
Object.assign(eventNames,{conditional_created:"已创建手动条件单",conditional_reserved:"条件满足 · 占用执行槽",conditional_executing:"条件单正在执行",conditional_cycle_finished:"条件单本轮结束",conditional_cancel_requested:"条件单取消请求",conditional_resumed:"条件单已恢复",conditional_paused:"条件单已暂停"});
eventNames.entry_direction_changed="挂单方向已切换";
const decimal = value => value === null || value === undefined ? "—" : String(value);
const fixed = value => value === null || value === undefined ? "—" : Number(value).toFixed(2);
const ms = value => value === null || value === undefined ? "—" : `${value} ms`;
const time = value => value ? new Date(value).toLocaleTimeString("zh-CN",{hour12:false}) : "—";
function set(id, value){$(id).textContent = value;}
function showError(message){$("error").hidden = !message;set("error",message || "");}
function error(message){actionError=message;showError(message);if($("dialog-error")){set("dialog-error",message || "");$("dialog-error").hidden=!message;}}
async function request(path, options = {}){
  const response = await fetch(path,{...options,cache:"no-store",signal:AbortSignal.timeout(options.method === "POST" ? 15000 : 4000)});
  const data=await response.json();
  if(!response.ok) throw new Error(data.error || `请求失败 (${response.status})`);
  return data;
}
function controls(){
  const state=current?.status || "STOPPED";
  $("start").disabled=!online || busy || ["STARTING","RUNNING","STOPPING"].includes(state);
  $("stop").disabled=!online || busy || !["STARTING","RUNNING"].includes(state);
  const running=online && !busy && state === "RUNNING";
  $("condition-create").disabled=!running || ["SAFE_MODE","ERROR"].includes(current?.state);
  for(const button of document.querySelectorAll("[data-condition-action]")) button.disabled=!running;
  if($("close-all"))$("close-all").disabled=!running || current?.state === "SAFE_MODE" || !(current?.conditions || []).some(c=>c.state === "OPEN");
}
async function action(name){
  busy = true;controls();
  try{
    const result = await request(`/api/${name}`,{method:"POST",headers:{"X-Control-Token":token}});
    current.status = result.status;
    if(name === "start"){points.length=0;sampleTime=0;}
    error(null);
  }catch(e){error(`操作未完成：${e.message}`);}
  finally{busy=false;controls();}
}
$("start").addEventListener("click",()=>action("start"));
$("stop").addEventListener("click",()=>action("stop"));
$("condition-form").addEventListener("submit",async event=>{
  event.preventDefault();
  if($("condition-create").disabled) return;
  const fields={direction:$("condition-direction").value,entry_threshold:$("condition-entry").value,cancel_threshold:$("condition-cancel").value,quantity:$("condition-quantity").value,repeat:$("condition-repeat").checked,exit_threshold:$("condition-exit").disabled?null:$("condition-exit").value || null};
  const signature=JSON.stringify(fields);
  if(!pendingDraft || pendingDraft.signature !== signature) pendingDraft={signature,id:crypto.randomUUID()};
  busy=true;controls();
  try{
    await request("/api/conditions",{method:"POST",headers:{"X-Control-Token":token,"Content-Type":"application/json"},body:JSON.stringify({...fields,request_id:pendingDraft.id})});
    pendingDraft=null;error(null);$("condition-dialog").close();
    current=await request("/api/status");render(current);
  }catch(e){error(`条件单未确认：${e.message}`);}
  finally{busy=false;controls();}
});
async function conditionAction(id,action){
  busy=true;controls();
  try{
    await request(`/api/conditions/${encodeURIComponent(id)}/${action}`,{method:"POST",headers:{"X-Control-Token":token}});
    error(null);current=await request("/api/status");render(current);
  }catch(e){error(e.message);}
  finally{busy=false;controls();}
}
function renderConditions(snapshot){
  if(!formInitialized){
    $("condition-entry").value=snapshot.config.entry_threshold;
    $("condition-cancel").value=snapshot.config.cancel_threshold;
    $("condition-quantity").value=snapshot.config.binance_qty;
    formInitialized=true;
  }
  const body=$("conditions-body");body.replaceChildren();
  const conditions=snapshot.conditions || [];
  set("waiting-count",conditions.filter(c=>c.state === "WAITING").length);
  set("active-count",conditions.filter(c=>["EXECUTING","CANCELING","CLOSING"].includes(c.state)).length);
  set("paused-count",conditions.filter(c=>c.state === "PAUSED").length);
  const order=snapshot.order;
  set("execution-slot",order ? `执行槽已占用 · 条件单 ${order.conditional_id?.slice(0,8) || "历史订单"} · ${labels[order.state]} · 其余条件单等待` : "执行槽空闲 · 只执行手动创建且满足条件的单子");
  if(snapshot.active_condition_id)set("execution-slot",`执行槽已占用 · ${snapshot.active_condition_id.slice(0,8)} · 其他条件单等待`);
  if(!conditions.length){const row=body.insertRow(),cell=row.insertCell();cell.colSpan=9;cell.className="empty-row";cell.textContent="尚无手动条件单，系统不会自动创建订单。";return;}
  for(const c of conditions.filter(c=>window.conditionVisible ? window.conditionVisible(c) : true)){
    let status=conditionLabels[c.state] || c.state;
    if(c.close_requested)status+=" · 平仓已排队";
    if(c.state === "WAITING"){
      status += order ? " · 等待执行槽" : !snapshot.quotes_valid ? ` · ${reasons[snapshot.quote_reason] || "等待有效行情"}` : ` · ${c.confirmation.count}/${snapshot.config.min_ticks} Tick · ${c.confirmation.duration_ms}/${snapshot.config.min_duration_ms} ms`;
    }
    const row=body.insertRow();row.dataset.conditionId=c.request_id;
    for(const value of [`${snapshot.config.symbols.binance} · ${c.request_id.slice(0,8)} / #${c.queue_seq}`,c.direction === "SHORT_BINANCE" ? "A · 空 Binance" : "B · 多 Binance",`${c.entry_threshold} / ${fixed(c.raw_spread)}`,c.cancel_threshold,c.quantity,c.repeat?"循环":"单次",c.execution_count,status]){const cell=row.insertCell();cell.textContent=String(value);}
    row.cells[1].className=c.direction === "SHORT_BINANCE"?"direction-a":"direction-b";
    row.cells[7].className=`condition-state state-${c.state.toLowerCase()}`;
    const actions=row.insertCell();
    for(const action of c.state === "PAUSED" ? ["resume","cancel"] : c.state === "WAITING" ? ["pause","cancel"] : c.state === "OPEN" ? ["close"] : ["EXECUTING","CANCELING"].includes(c.state) ? ["cancel"] : []){
      const button=document.createElement("button");button.textContent=({resume:"恢复",cancel:"取消",pause:"暂停",close:"平仓"})[action];button.dataset.conditionAction=action;button.addEventListener("click",()=>conditionAction(c.request_id,action));actions.append(button);
    }
    for(const [label,handler] of [["复制",()=>window.openCondition(c)],["详情",()=>window.showCondition(c)]]){const button=document.createElement("button");button.textContent=label;button.addEventListener("click",handler);actions.append(button);}
  }
  if(!body.rows.length){const cell=body.insertRow().insertCell();cell.colSpan=9;cell.className="empty-row";cell.textContent="此筛选下暂无条件单";}
}
function quoteView(venue, snapshot){
  const q = snapshot[venue], connected = snapshot.connections[venue];
  const age = q ? Math.max(snapshot.timestamp_ms-q.local_ts_ms,snapshot.timestamp_ms-q.exchange_ts_ms) : null;
  const fresh = connected && age !== null && snapshot.timestamp_ms >= q.local_ts_ms && snapshot.timestamp_ms >= q.exchange_ts_ms && age <= snapshot.config.max_quote_age_ms;
  set(`${venue}-bid`,decimal(q?.bid));set(`${venue}-ask`,decimal(q?.ask));
  set(`${venue}-latency`,q ? ms(q.local_ts_ms-q.exchange_ts_ms) : "—");
  set(`${venue}-age`,ms(age));
  set(`${venue}-connection`,!connected ? "未连接" : fresh ? "行情有效" : "报价过期 / 时间异常");
  $(`${venue}-connection`).className=`connection ${connected ? fresh ? "fresh" : "stale" : ""}`;
  $(`${venue}-card`).classList.toggle("inactive",!fresh);
  if(venue === "binance"){
    set("binance-bid-qty",`数量 ${decimal(q?.bid_qty)}`);set("binance-ask-qty",`数量 ${decimal(q?.ask_qty)}`);
  }
}
function signal(prefix,direction,snapshot){
  set(`${prefix}-spread`,fixed(snapshot.directions[direction].raw_spread));
  set(`${prefix}-state`,snapshot.quotes_valid ? "实时报价 · 条件按各单独立判断" : `${reasons[snapshot.quote_reason] || "等待行情"} · 仅供观察`);
}
function renderEvents(events){
  const list=$("events-list");list.replaceChildren();
  if(!events.length){const p=document.createElement("p");p.className="empty-message";p.textContent="暂无事件。启动后显示连接、信号和模拟订单记录。";list.append(p);return;}
  for(const e of [...events].reverse()){
    const row=document.createElement("div");row.className=`event-row ${e.event.startsWith("quote_") || e.level === "ERROR"?"error-event":""}`;
    const t=document.createElement("time");t.textContent=time(e.timestamp_ms);
    const content=document.createElement("div"),title=document.createElement("strong"),detail=document.createElement("p");
    title.textContent=eventNames[e.event] || e.event;title.title=e.event;
    detail.textContent=e.error || [e.reason,e.direction,e.condition ? `条件 ${e.condition.request_id.slice(0,8)} · ${conditionLabels[e.condition.state] || e.condition.state}` : null,e.order?.price ? `价格 ${e.order.price}` : null,e.order_id?.slice(0,12)].filter(Boolean).join(" · ") || e.event;
    content.append(title,detail);row.append(t,content);list.append(row);
  }
}
async function history(){
  try{
    const data=await request("/api/history"), body=$("orders-body");body.replaceChildren();
    set("order-count",data.orders.length);$("history-error").hidden=true;
    if(!data.orders.length){const row=document.createElement("tr"),cell=document.createElement("td");cell.colSpan=6;cell.className="empty-row";cell.textContent="暂无模拟订单。满足连续确认条件后，订单会出现在这里。";row.append(cell);body.append(row);return;}
    for(const order of data.orders){const row=document.createElement("tr");for(const value of [new Date(order.created_at_ms).toLocaleString("zh-CN",{hour12:false}),order.order_id.slice(0,12),order.direction === "SHORT_BINANCE"?"A · SELL Binance":"B · BUY Binance",order.price,order.quantity,labels[order.state]||order.state]){const cell=document.createElement("td");cell.textContent=value;row.append(cell);}body.append(row);}
  }catch(e){$("history-error").hidden=false;set("history-error",`历史订单暂不可用：${e.message}`);}
}
function render(snapshot){
  if(window.renderTrading)window.renderTrading(snapshot);
  const c=snapshot.config;set("clock",new Date(snapshot.timestamp_ms).toLocaleString("zh-CN",{hour12:false}));
  set("session-state",labels[snapshot.status]||snapshot.status);set("strategy-state",labels[snapshot.state]||snapshot.state);
  $("session-dot").className=`status-dot ${snapshot.status === "RUNNING"?"running":snapshot.status === "ERROR"?"error":""}`;
  set("binance-symbol",c.symbols.binance);set("mt5-symbol",c.symbols.mt5);
  quoteView("binance",snapshot);quoteView("mt5",snapshot);
  renderConditions(snapshot);
  set("confirmation-rule",`${c.min_ticks} ticks + ${c.min_duration_ms} ms`);set("age-limit",ms(c.max_quote_age_ms));set("skew-limit",ms(c.max_quote_skew_ms));set("pending-limit",ms(c.max_pending_ms));
  set("quote-validity",snapshot.quotes_valid ? "双边报价有效" : reasons[snapshot.quote_reason] || "等待有效行情");
  $("quote-validity").className=`validity ${snapshot.quotes_valid?"good":""}`;
  set("quote-skew",snapshot.binance && snapshot.mt5 ? ms(Math.abs(snapshot.binance.exchange_ts_ms-snapshot.mt5.exchange_ts_ms)):"—");
  signal("a","SHORT_BINANCE",snapshot);signal("b","LONG_BINANCE",snapshot);
  set("pair-a",fixed(snapshot.directions.SHORT_BINANCE.raw_spread));set("pair-b",fixed(snapshot.directions.LONG_BINANCE.raw_spread));
  const o=snapshot.order;
  set("current-order",o?`当前 ${o.direction === "SHORT_BINANCE"?"SELL":"BUY"} · ${o.price} × ${o.quantity} · ${labels[o.state]||o.state}`:"当前没有挂单");
  renderEvents(snapshot.events);showError(snapshot.error || actionError);
  if(snapshot.timestamp_ms-sampleTime>=450){points.push({t:snapshot.timestamp_ms,a:snapshot.quotes_valid ? snapshot.directions.SHORT_BINANCE.raw_spread : null,b:snapshot.quotes_valid ? snapshot.directions.LONG_BINANCE.raw_spread : null});if(points.length>120) points.shift();sampleTime=snapshot.timestamp_ms;}
  draw();set("last-update",`· 更新于 ${time(snapshot.timestamp_ms)}`);
}
function draw(){
  const canvas=$("spread-chart"), box=canvas.getBoundingClientRect(), ratio=window.devicePixelRatio||1;
  canvas.width=Math.round(box.width*ratio);canvas.height=Math.round(box.height*ratio);
  const ctx=canvas.getContext("2d");ctx.scale(ratio,ratio);
  const width=box.width,height=box.height,left=37,right=12,top=15,bottom=25;
  const values=points.flatMap(p=>[p.a,p.b]).filter(v=>v!==null).map(Number);
  $("chart-empty").hidden=values.length>0;
  let low=Math.floor(Math.min(-5,...values)-.5),high=Math.ceil(Math.max(5,...values)+.5);
  const y=v=>top+(high-v)/(high-low)*(height-top-bottom);
  const x=i=>left+i/Math.max(points.length-1,1)*(width-left-right);
  ctx.font="10px Consolas, monospace";ctx.textAlign="right";
  for(let i=0;i<=4;i++){const value=low+(high-low)*i/4,py=y(value);ctx.strokeStyle="#edf1f3";ctx.lineWidth=1;ctx.beginPath();ctx.moveTo(left,py);ctx.lineTo(width-right,py);ctx.stroke();ctx.fillStyle="#a4b1ba";ctx.fillText(value.toFixed(1),left-9,py+3);}
  for(const [key,color] of [["a","#238f7c"],["b","#7c96c0"]]){ctx.strokeStyle=color;ctx.lineWidth=1.8;ctx.beginPath();let active=false;points.forEach((p,i)=>{if(p[key]===null){active=false;return;}if(active)ctx.lineTo(x(i),y(Number(p[key])));else ctx.moveTo(x(i),y(Number(p[key])));active=true;});ctx.stroke();}
  if(points.length){ctx.fillStyle="#a4b1ba";ctx.textAlign="left";ctx.fillText(time(points[0].t),left,height-7);ctx.textAlign="right";ctx.fillText(time(points.at(-1).t),width-right,height-7);}
}
new ResizeObserver(draw).observe($("spread-chart").parentElement);
async function poll(){
  try{
    current=await request("/api/status");token=current.control_token;online=true;
    set("web-state","● 本地面板已连接");$("web-state").className="web-state";render(current);
    if(Date.now()-lastHistory>5000){lastHistory=Date.now();await history();}
  }catch(e){online=false;$("session-dot").className="status-dot error";set("session-state","OFFLINE");set("strategy-state","");set("web-state","● 面板连接中断");$("web-state").className="web-state offline";set("quote-validity","连接中断 · 数据已冻结");$("quote-validity").className="validity";for(const venue of ["binance","mt5"]){$(`${venue}-card`).classList.add("inactive");set(`${venue}-connection`,"面板离线");$(`${venue}-connection`).className="connection";}error(`无法连接本地监控服务：${e.message}。最后显示的数据已冻结，请检查服务是否运行。`);}
  controls();setTimeout(poll,500);
}
poll();
