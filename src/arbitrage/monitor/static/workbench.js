"use strict";
let queueFilter="active";
let renderedMode=null;
window.renderTrading=s=>{
  const symbols=s.config.symbols;
  const pair=document.querySelector(".pair-card>strong");
  pair.firstChild.textContent=symbols.binance+" ";
  pair.querySelector("small").textContent=symbols.mt5+" / "+symbols.binance;
  document.querySelector(".eyebrow").textContent=symbols.binance+" · CROSS-VENUE ARBITRAGE";
  document.querySelector("h1").firstChild.textContent=(symbols.binance.startsWith("BTC")?"比特币":symbols.binance.startsWith("XAU")?"黄金":symbols.binance)+"跨市场监控 ";
  document.title=symbols.binance+" · 跨市场监控";
  const live=s.mode === "live";
  if(renderedMode!==s.mode){
    renderedMode=s.mode;
    document.querySelector(".badge").textContent=live?"LIVE · 实盘":"PAPER";
    document.querySelector(".badge").classList.toggle("live-mode",live);
    $("condition-exit").disabled=!live;
    $("live-positions").hidden=!live;
    if(live){
      conditionLabels.EXECUTING="真实挂单 / 对冲中";conditionLabels.DONE="本轮结束";
      $("start").textContent="启动实盘会话";$("stop").textContent="停止入场与监控";
      document.querySelector(".subtitle").textContent="Binance Futures ↔ MetaTrader 5 · 真实账户执行";
      document.querySelector(".manual-panel>.manual-note").textContent="Binance Maker 成交后按实际数量对冲 MT5。入场和平仓共用单执行槽；循环单完成平仓后再排队。停止监控保留已开仓持仓，请先处理需关闭的仓位。";
      $("paper").querySelector("h2").textContent="实盘会话";
      $("paper").querySelector("p").textContent="本页面创建的条件单满足条件后，会使用真实账户资金执行。";
      $("paper").querySelector("ul").replaceChildren();
      $("paper").querySelector(".paper-bottom").textContent="平仓价差：A = Binance Ask − MT5 Bid；B = MT5 Ask − Binance Bid。可设置达到目标后平仓，也可逐笔点击平仓。两边资金使用不同币种，分别展示收益。";
      document.querySelector("footer>span").textContent="AURUM · 本地实盘工作台";
      $("orders").hidden=true;
      document.querySelector('nav a[href="#orders"]').href="#live-positions";
    }
  }
  const cards=document.querySelectorAll(".account-strip article");
  for(const [i,key] of [[0,"mt5"],[1,"binance"]]){
    const a=s.accounts?.[key];cards[i].querySelector("p").textContent=a?"权益 "+a.equity+" / 可用 "+a.available+" / 浮盈 "+a.profit+" "+a.currency:"等待账户数据";
    cards[i].querySelector("small").textContent=!a && key === "binance" && !live?"Paper 使用公开行情，未读取 Binance 私有账户":s.accounts_error || "只读账户采样 "+time(s.accounts_updated_ms)+" · "+Math.max(0,s.timestamp_ms-(s.accounts_updated_ms || 0))+" ms 前";
  }
  if(!live)return;
  cards[2].querySelector("small").textContent="本品种独立账本 · 资金费尚未按交易对分摊";
  const container=$("live-trades");container.replaceChildren();
  for(const t of [...(s.trades || [])].reverse().slice(0,50)){
    const card=document.createElement("article");card.className="live-trade-card";
    const title=document.createElement("strong");title.textContent=(t.direction === "SHORT_BINANCE"?"A":"B")+" · "+t.trade_id.slice(0,8)+" · "+({OPEN:"已开仓",CLOSED:"已平仓",REVIEW:"待核对",CANCELED:"未成交已撤",FAILED:"执行失败"}[t.state] || t.state);
    const text=document.createElement("p");text.textContent="Binance 已成交 "+t.filled_qty+" / "+t.quantity+"　MT5 "+(t.mt5_open?.volume || "0")+" 手　开仓信号价差 "+t.entry_spread+"　平仓目标 "+(s.conditions.find(c=>c.request_id===t.condition_id)?.exit_threshold ?? "手动");
    const pnl=document.createElement("p");pnl.textContent=t.settlement?.summary?"已实现 Binance "+t.settlement.summary.binance_net+" USDT（未扣其他币种手续费及资金费） / MT5 "+t.settlement.summary.mt5_net+" "+t.settlement.mt5_currency:"已实现收益 / 手续费：等待完整成交回报";
    card.append(title,text,pnl);
    if(t.settlement?.summary){const fee=document.createElement("p");fee.textContent="手续费 Binance："+Object.entries(t.settlement.summary.binance_fees).map(([asset,value])=>value+" "+asset).join(" / ")+"；MT5 佣金及费用记账："+t.settlement.summary.mt5_fees+" "+t.settlement.mt5_currency+"；Binance 资金费未分摊";card.append(fee);}
    if(t.error){const err=document.createElement("p");err.textContent=t.error;card.append(err);}
    if(t.state === "OPEN"){const button=document.createElement("button");button.textContent="双边平仓";button.dataset.conditionAction="close";button.addEventListener("click",()=>conditionAction(t.condition_id,"close"));card.append(button);}
    container.append(card);
  }
  if(!container.children.length)container.textContent="暂无双边交易。创建条件单后等待触发。";
};
const finishedStates=new Set(["DONE","CANCELED","FAILED","CLOSED"]);
window.conditionVisible=c=>(queueFilter === "all" || finishedStates.has(c.state) === (queueFilter === "finished")) && ($("queue-direction").value === "all" || $("queue-direction").value === c.direction);
window.openCondition=(source=null)=>{
  if(source)pendingDraft=null;error(null);
  if(source){$("condition-direction").value=source.direction;$("condition-entry").value=source.entry_threshold;$("condition-cancel").value=source.cancel_threshold;$("condition-quantity").value=source.quantity;$("condition-repeat").checked=source.repeat;$("condition-exit").value=source.exit_threshold ?? "";}
  set("condition-title",source?"复制为新的条件挂单":"添加条件挂单");
  $("condition-dialog").showModal();$("condition-entry").focus();
};
$("open-condition").addEventListener("click",()=>window.openCondition());
$("close-all").addEventListener("click",async()=>{
  if($("close-all").disabled)return;
  busy=true;controls();
  try{await request("/api/close-all",{method:"POST",headers:{"X-Control-Token":token}});error(null);current=await request("/api/status");render(current);}
  catch(e){error(e.message);}
  finally{busy=false;controls();}
});
$("close-condition").addEventListener("click",()=>$("condition-dialog").close());
for(const button of document.querySelectorAll("[data-new-direction]")) button.addEventListener("click",()=>{window.openCondition();$("condition-direction").value=button.dataset.newDirection;});
for(const button of document.querySelectorAll("[data-queue-filter]")) button.addEventListener("click",()=>{queueFilter=button.dataset.queueFilter;for(const peer of document.querySelectorAll("[data-queue-filter]"))peer.setAttribute("aria-pressed",String(peer===button));if(current){renderConditions(current);controls();}});
$("queue-direction").addEventListener("change",()=>{if(current){renderConditions(current);controls();}});
const detail=document.createElement("dialog");detail.className="condition-detail";detail.setAttribute("aria-label","条件单详情");document.body.append(detail);
window.showCondition=c=>{
  detail.replaceChildren();const heading=document.createElement("h2");heading.textContent="条件单详情";const list=document.createElement("dl");
  for(const [label,value] of [["完整编号",c.request_id],["方向",c.direction === "SHORT_BINANCE"?"A · 空 Binance / 多 MT5":"B · 多 Binance / 空 MT5"],["创建时间",new Date(c.created_at_ms).toLocaleString("zh-CN")],["更新时间",new Date(c.updated_at_ms).toLocaleString("zh-CN")],["执行订单",c.execution_order_id || "尚未执行"],["最近结果",c.last_result || "—"],["当前超出阈值",decimal(c.edge)]]){const dt=document.createElement("dt"),dd=document.createElement("dd");dt.textContent=label;dd.textContent=value;list.append(dt,dd);}
  const close=document.createElement("button");close.textContent="关闭";close.addEventListener("click",()=>detail.close());detail.append(heading,list,close);detail.showModal();
};
