(() => {
  "use strict";
  const el = (id) => document.getElementById(id);
  const integer = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });
  const decimal = new Intl.NumberFormat("en-US", { minimumFractionDigits: 1, maximumFractionDigits: 1 });
  const clock = new Intl.DateTimeFormat([], { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false });
  const state = { socket: null, timer: null, paused: false, destroyed: false, attempt: 0, epoch: "", sequence: null, count: 0, gaps: 0n, trades: [], fills: 0, points: [], arrivals: [], startedAt: 0 };
  const canvas = el("price-chart");
  const context = canvas.getContext("2d");
  const rowCount = 6;

  function status(mode, text) {
    el("connection").dataset.state = mode;
    el("connection-label").textContent = text;
  }
  function notice(text) { el("notice").textContent = text; el("notice").hidden = !text; }
  function depth(id, levels) {
    const body = el(id);
    const rows = [];
    const highest = Math.max(1, ...levels.map((level) => Number(level.quantity)));
    for (let index = 0; index < Math.max(rowCount, levels.length); index += 1) {
      const row = document.createElement("tr");
      const level = levels[index];
      if (level) row.style.setProperty("--depth", `${Math.min(100, Number(level.quantity) / highest * 100)}%`);
      else row.className = "empty-row";
      for (const value of level ? [level.price, level.quantity, level.orders] : [null, null, null]) {
        const cell = document.createElement("td");
        cell.textContent = value === null ? "—" : integer.format(Number(value));
        row.append(cell);
      }
      rows.push(row);
    }
    body.replaceChildren(...rows);
    el(id === "bids" ? "bid-levels" : "ask-levels").textContent = `${levels.length} LEVEL${levels.length === 1 ? "" : "S"}`;
  }
  function drawChart() {
    if (!context) return;
    const rect = canvas.getBoundingClientRect();
    if (!rect.width || !rect.height) return;
    const ratio = Math.min(window.devicePixelRatio || 1, 2);
    canvas.width = Math.round(rect.width * ratio);
    canvas.height = Math.round(rect.height * ratio);
    context.setTransform(ratio, 0, 0, ratio, 0, 0);
    const width = rect.width;
    const height = rect.height;
    const left = 1;
    const right = width - 61;
    const top = 14;
    const bottom = height - 25;
    const prices = state.points.map((point) => point.price);
    const low = prices.length ? Math.min(...prices) : 0;
    const high = prices.length ? Math.max(...prices) : 1;
    const padding = Math.max(1, (high - low) * .2);
    const minimum = low - padding;
    const maximum = high + padding;
    const y = (price) => bottom - (price - minimum) / (maximum - minimum) * (bottom - top);
    context.font = "9px Consolas, monospace";
    for (let index = 0; index < 4; index += 1) {
      const lineY = top + (bottom - top) * index / 3;
      context.strokeStyle = "#303830";
      context.setLineDash([3, 5]);
      context.beginPath(); context.moveTo(left, lineY); context.lineTo(right, lineY); context.stroke();
      context.fillStyle = "#85917e";
      context.fillText(prices.length ? decimal.format(maximum - (maximum - minimum) * index / 3) : "—", right + 9, lineY + 3);
    }
    context.setLineDash([]);
    if (state.points.length < 2) return;
    const x = (index) => left + index / (state.points.length - 1) * (right - left);
    const gradient = context.createLinearGradient(0, top, 0, bottom);
    gradient.addColorStop(0, "#b7ef732e"); gradient.addColorStop(1, "#b7ef7300");
    context.beginPath(); context.moveTo(left, bottom);
    state.points.forEach((point, index) => context.lineTo(x(index), y(point.price)));
    context.lineTo(right, bottom); context.closePath(); context.fillStyle = gradient; context.fill();
    context.beginPath(); state.points.forEach((point, index) => index ? context.lineTo(x(index), y(point.price)) : context.moveTo(x(index), y(point.price)));
    context.strokeStyle = "#b7ef73"; context.lineWidth = 1.6; context.stroke();
    context.beginPath(); context.arc(right, y(state.points.at(-1).price), 3, 0, Math.PI * 2); context.fillStyle = "#b7ef73"; context.fill();
    context.fillStyle = "#85917e";
    context.fillText(clock.format(state.points[0].time), left, height - 6);
    const lastTime = clock.format(state.points.at(-1).time);
    context.fillText(lastTime, right - context.measureText(lastTime).width, height - 6);
  }
  function renderTrade(trade, timestamp) {
    state.fills += 1;
    state.trades.unshift({ price: trade.price, quantity: trade.quantity, side: trade.aggressorSide === "BUY" ? "buy" : "sell", time: timestamp });
    state.trades = state.trades.slice(0, 6);
    const rows = state.trades.map((item) => {
      const row = document.createElement("li");
      const price = document.createElement("span"); price.className = `trade-price ${item.side}`; price.textContent = integer.format(item.price);
      const size = document.createElement("small"); size.textContent = `× ${integer.format(item.quantity)}`; price.append(size);
      const side = document.createElement("span"); side.className = `trade-side ${item.side}`;
      const badge = document.createElement("span"); badge.textContent = item.side.toUpperCase(); side.append(badge);
      const time = document.createElement("span"); time.className = "trade-time"; time.textContent = clock.format(item.time);
      row.append(price, side, time); return row;
    });
    el("trades").replaceChildren(...rows); el("empty-trades").hidden = true;
    el("trade-total").textContent = `${integer.format(state.fills)} fill${state.fills === 1 ? "" : "s"}`;
  }
  function receive(message) {
    if (state.paused || state.destroyed) return;
    let event;
    let sequence;
    try {
      event = JSON.parse(message.data);
      sequence = BigInt(event.sequence || "0");
      if (!event.epoch || !Array.isArray(event.bids) || !Array.isArray(event.asks)) return;
    } catch { return; }
    if (event.epoch !== state.epoch) {
      state.epoch = event.epoch; state.sequence = null; state.points = []; state.trades = []; state.fills = 0;
      el("trades").replaceChildren(); el("empty-trades").hidden = false; el("trade-total").textContent = "0 fills";
    }
    if (state.sequence !== null && sequence <= state.sequence) return;
    if (state.sequence !== null && sequence > state.sequence + 1n) state.gaps += sequence - state.sequence - 1n;
    state.sequence = sequence; state.count += 1; state.attempt = 0;
    const now = Date.now();
    if (!state.startedAt) state.startedAt = now;
    state.arrivals.push(now); state.arrivals = state.arrivals.filter((time) => now - time < 5000);
    status("live", "Feed connected"); notice("");
    el("event-count").textContent = integer.format(state.count);
    el("sequence").textContent = integer.format(sequence);
    el("gaps").textContent = integer.format(state.gaps);
    el("epoch").textContent = `${state.epoch.slice(0, 12)}…`; el("epoch").title = state.epoch;
    const bids = event.bids.slice(0, 10); const asks = event.asks.slice(0, 10);
    depth("bids", bids); depth("asks", asks);
    el("best-bid").textContent = bids.length ? integer.format(bids[0].price) : "—";
    el("best-ask").textContent = asks.length ? integer.format(asks[0].price) : "—";
    el("spread").textContent = bids.length && asks.length ? integer.format(asks[0].price - bids[0].price) : "—";
    if (bids.length && asks.length) {
      const price = (Number(bids[0].price) + Number(asks[0].price)) / 2;
      state.points.push({ price, time: now }); state.points = state.points.slice(-60);
      el("midpoint").textContent = decimal.format(price);
      el("chart-empty").hidden = state.points.length > 1; drawChart();
    } else {
      el("midpoint").textContent = "—";
      el("chart-empty").textContent = "Waiting for both sides of the book…"; el("chart-empty").hidden = false;
    }
    if (event.trade) renderTrade(event.trade, now);
  }
  function connect() {
    if (state.paused || state.destroyed) return;
    status("connecting", state.attempt ? "Reconnecting" : "Connecting");
    const socket = new WebSocket(`${location.protocol === "https:" ? "wss:" : "ws:"}//${location.host}/ws`);
    state.socket = socket;
    socket.onmessage = (message) => { if (state.socket === socket) receive(message); };
    socket.onopen = () => { status("connecting", "Waiting for depth"); };
    socket.onerror = () => { socket.close(); };
    socket.onclose = () => {
      if (state.paused || state.destroyed) return;
      state.arrivals = [];
      status("offline", "Reconnecting");
      notice("Waiting for the live engine. Free hosting can take about a minute to wake up. Reconnecting automatically.");
      const delay = Math.min(20000, 1000 * 2 ** Math.min(state.attempt++, 5));
      state.timer = setTimeout(connect, delay + Math.random() * 300);
    };
  }
  el("pause").addEventListener("click", () => {
    state.paused = !state.paused;
    clearTimeout(state.timer);
    el("pause-label").textContent = state.paused ? "Resume feed" : "Pause feed";
    el("pause-symbol").textContent = state.paused ? "▷" : "Ⅱ";
    if (state.paused) {
      state.socket?.close(); state.arrivals = [];
      status("paused", "Feed paused"); notice("Feed paused. Displayed depth is the last received snapshot. Resume to fetch current depth.");
    } else { notice(""); connect(); }
  });
  setInterval(() => {
    const now = Date.now();
    state.arrivals = state.arrivals.filter((time) => now - time < 5000);
    const span = Math.max(1, Math.min(5, (now - state.startedAt) / 1000));
    el("event-rate").textContent = decimal.format(state.arrivals.length / span);
  }, 1000);
  if ("ResizeObserver" in window) new ResizeObserver(drawChart).observe(canvas);
  else window.addEventListener("resize", drawChart);
  window.addEventListener("pagehide", () => { state.destroyed = true; clearTimeout(state.timer); state.socket?.close(); });
  depth("bids", []); depth("asks", []); drawChart(); connect();
})();
