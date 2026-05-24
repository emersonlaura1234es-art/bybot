"""
BYBOT.ai — Backend FastAPI para Day Trade na Bybit
Integração real com a API v5 da Bybit (REST + WebSocket)
"""

import hashlib
import hmac
import time
import json
import asyncio
import logging
from datetime import datetime
from typing import Optional, Dict, List
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ── logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bybot")

# ── configuração ───────────────────────────────────────────────────────────────
BYBIT_BASE = "https://api.bybit.com"          # produção
# BYBIT_BASE = "https://api-testnet.bybit.com" # testnet (recomendado para testes!)

RECV_WINDOW = 5000  # ms

# Estado global do bot
state: Dict = {
    "bot_active": False,
    "open_position": None,
    "orders": [],
    "balance": 0.0,
    "pnl_today": 0.0,
    "last_signal": None,
}

# ── utilitários de assinatura Bybit ────────────────────────────────────────────
def make_signature(api_secret: str, params: str) -> str:
    return hmac.new(
        api_secret.encode("utf-8"),
        params.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def build_headers(api_key: str, api_secret: str, body: str = "") -> dict:
    ts = str(int(time.time() * 1000))
    sign_str = ts + api_key + str(RECV_WINDOW) + body
    signature = make_signature(api_secret, sign_str)
    return {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-SIGN": signature,
        "X-BAPI-RECV-WINDOW": str(RECV_WINDOW),
        "Content-Type": "application/json",
    }


# ── cliente Bybit ──────────────────────────────────────────────────────────────
class BybitClient:
    def __init__(self, api_key: str, api_secret: str):
        self.key = api_key
        self.secret = api_secret

    async def _get(self, path: str, params: dict = None) -> dict:
        params = params or {}
        query = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
        ts = str(int(time.time() * 1000))
        sign_str = ts + self.key + str(RECV_WINDOW) + query
        headers = {
            "X-BAPI-API-KEY": self.key,
            "X-BAPI-TIMESTAMP": ts,
            "X-BAPI-SIGN": make_signature(self.secret, sign_str),
            "X-BAPI-RECV-WINDOW": str(RECV_WINDOW),
        }
        url = f"{BYBIT_BASE}{path}"
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(url, params=params, headers=headers)
            return r.json()

    async def _post(self, path: str, body: dict) -> dict:
        body_str = json.dumps(body)
        headers = build_headers(self.key, self.secret, body_str)
        url = f"{BYBIT_BASE}{path}"
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.post(url, content=body_str, headers=headers)
            return r.json()

    # ── mercado ────────────────────────────────────────────────────────────────
    async def get_ticker(self, symbol: str) -> dict:
        """Preço atual e volume"""
        r = await self._get("/v5/market/tickers", {"category": "linear", "symbol": symbol})
        if r.get("retCode") != 0:
            raise Exception(f"Ticker error: {r}")
        return r["result"]["list"][0]

    async def get_klines(self, symbol: str, interval: str = "5", limit: int = 100) -> list:
        """Candles OHLCV"""
        r = await self._get("/v5/market/kline", {
            "category": "linear",
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
        })
        if r.get("retCode") != 0:
            raise Exception(f"Kline error: {r}")
        return r["result"]["list"]  # [[ts, open, high, low, close, volume, turnover], ...]

    # ── conta ──────────────────────────────────────────────────────────────────
    async def get_wallet(self, coin: str = "USDT") -> float:
        """Saldo disponível em USDT"""
        r = await self._get("/v5/account/wallet-balance", {"accountType": "UNIFIED", "coin": coin})
        if r.get("retCode") != 0:
            raise Exception(f"Wallet error: {r}")
        coins = r["result"]["list"][0]["coin"]
        for c in coins:
            if c["coin"] == coin:
                return float(c["availableToWithdraw"])
        return 0.0

    async def get_positions(self, symbol: str) -> list:
        """Posições abertas"""
        r = await self._get("/v5/position/list", {"category": "linear", "symbol": symbol})
        if r.get("retCode") != 0:
            raise Exception(f"Position error: {r}")
        return r["result"]["list"]

    # ── ordens ─────────────────────────────────────────────────────────────────
    async def set_leverage(self, symbol: str, leverage: int) -> dict:
        return await self._post("/v5/position/set-leverage", {
            "category": "linear",
            "symbol": symbol,
            "buyLeverage": str(leverage),
            "sellLeverage": str(leverage),
        })

    async def place_order(
        self,
        symbol: str,
        side: str,       # "Buy" ou "Sell"
        qty: float,
        order_type: str = "Market",
        tp_price: Optional[float] = None,
        sl_price: Optional[float] = None,
    ) -> dict:
        body = {
            "category": "linear",
            "symbol": symbol,
            "side": side,
            "orderType": order_type,
            "qty": str(round(qty, 3)),
            "timeInForce": "GTC",
            "reduceOnly": False,
            "closeOnTrigger": False,
        }
        if tp_price:
            body["takeProfit"] = str(round(tp_price, 2))
            body["tpTriggerBy"] = "MarkPrice"
        if sl_price:
            body["stopLoss"] = str(round(sl_price, 2))
            body["slTriggerBy"] = "MarkPrice"

        r = await self._post("/v5/order/create", body)
        log.info(f"ORDER → {side} {qty} {symbol} | resp: {r}")
        return r

    async def close_position(self, symbol: str, side: str, qty: float) -> dict:
        """Fecha posição com ordem de mercado inversa"""
        close_side = "Sell" if side == "Buy" else "Buy"
        body = {
            "category": "linear",
            "symbol": symbol,
            "side": close_side,
            "orderType": "Market",
            "qty": str(round(qty, 3)),
            "reduceOnly": True,
            "timeInForce": "GTC",
        }
        return await self._post("/v5/order/create", body)

    async def cancel_all_orders(self, symbol: str) -> dict:
        return await self._post("/v5/order/cancel-all", {
            "category": "linear",
            "symbol": symbol,
        })


# ── motor de sinais técnicos ───────────────────────────────────────────────────
def calc_rsi(closes: List[float], period: int = 14) -> float:
    if len(closes) < period + 1:
        return 50.0
    changes = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    recent = changes[-period:]
    gains = sum(c for c in recent if c > 0) / period
    losses = abs(sum(c for c in recent if c < 0)) / period
    if losses == 0:
        return 100.0
    rs = gains / losses
    return round(100 - 100 / (1 + rs), 2)


def calc_ema(closes: List[float], period: int) -> float:
    if len(closes) < period:
        return closes[-1]
    k = 2 / (period + 1)
    ema = sum(closes[:period]) / period
    for price in closes[period:]:
        ema = price * k + ema * (1 - k)
    return round(ema, 4)


def compute_signal(closes: List[float]) -> dict:
    rsi = calc_rsi(closes)
    ema9 = calc_ema(closes, 9)
    ema21 = calc_ema(closes, 21)
    last = closes[-1]
    prev = closes[-6] if len(closes) >= 6 else last
    momentum = round(((last - prev) / prev) * 100, 4)

    score = 50.0
    if rsi < 35:
        score += 20
    elif rsi > 65:
        score -= 20
    if ema9 > ema21:
        score += 15
    else:
        score -= 15
    if momentum > 0.1:
        score += 10
    elif momentum < -0.1:
        score -= 10

    signal = "LONG" if score >= 65 else ("SHORT" if score <= 35 else "NEUTRO")
    confidence = min(95.0, abs(score - 50) * 2 + 40)

    return {
        "signal": signal,
        "confidence": round(confidence, 1),
        "rsi": rsi,
        "ema9": ema9,
        "ema21": ema21,
        "momentum": momentum,
        "score": round(score, 1),
        "price": last,
    }


# ── loop do bot ────────────────────────────────────────────────────────────────
async def bot_loop(cfg: dict):
    """Loop principal — roda em background enquanto bot_active=True"""
    symbol = cfg["symbol"]
    api_key = cfg["api_key"]
    api_secret = cfg["api_secret"]
    leverage = cfg["leverage"]
    order_size_usdt = cfg["order_size_usdt"]
    min_confidence = cfg.get("min_confidence", 68.0)
    tp_pct = cfg.get("tp_pct", 1.5) / 100
    sl_pct = cfg.get("sl_pct", 0.8) / 100

    client = BybitClient(api_key, api_secret)

    log.info(f"🤖 Bot iniciado | {symbol} | lev={leverage}x | size=${order_size_usdt}")

    try:
        await client.set_leverage(symbol, leverage)
    except Exception as e:
        log.warning(f"set_leverage: {e}")

    while state["bot_active"]:
        try:
            # 1. buscar candles
            klines = await client.get_klines(symbol, interval="5", limit=100)
            closes = [float(k[4]) for k in reversed(klines)]  # close price, ordem cronológica
            current_price = closes[-1]

            # 2. calcular sinal
            sig = compute_signal(closes)
            state["last_signal"] = {**sig, "ts": datetime.now().isoformat()}
            log.info(f"📊 {symbol} ${current_price:.2f} | {sig['signal']} {sig['confidence']}% | RSI={sig['rsi']}")

            # 3. verificar posição aberta
            positions = await client.get_positions(symbol)
            has_position = any(float(p.get("size", 0)) > 0 for p in positions)

            # 4. abrir posição se sinal forte e sem posição aberta
            if not has_position and sig["signal"] != "NEUTRO" and sig["confidence"] >= min_confidence:
                side = "Buy" if sig["signal"] == "LONG" else "Sell"
                qty = round(order_size_usdt / current_price, 3)

                tp = current_price * (1 + tp_pct) if side == "Buy" else current_price * (1 - tp_pct)
                sl = current_price * (1 - sl_pct) if side == "Buy" else current_price * (1 + sl_pct)

                result = await client.place_order(
                    symbol=symbol,
                    side=side,
                    qty=qty,
                    tp_price=tp,
                    sl_price=sl,
                )

                if result.get("retCode") == 0:
                    order_id = result["result"]["orderId"]
                    entry = {
                        "id": order_id,
                        "side": side,
                        "qty": qty,
                        "entry": current_price,
                        "tp": round(tp, 2),
                        "sl": round(sl, 2),
                        "symbol": symbol,
                        "time": datetime.now().strftime("%H:%M:%S"),
                        "status": "ABERTA",
                    }
                    state["open_position"] = entry
                    state["orders"].insert(0, entry)
                    state["orders"] = state["orders"][:50]
                    log.info(f"✅ Ordem aberta: {side} {qty} {symbol} @ {current_price}")
                else:
                    log.error(f"❌ Erro ao abrir ordem: {result}")

            # 5. atualizar saldo
            try:
                state["balance"] = await client.get_wallet()
            except Exception:
                pass

        except Exception as e:
            log.error(f"Erro no loop: {e}")

        # aguarda 30 segundos antes do próximo ciclo
        await asyncio.sleep(30)

    log.info("🛑 Bot encerrado")


# ── FastAPI app ────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("🚀 BYBOT.ai Backend iniciado")
    yield
    log.info("👋 Encerrando...")

app = FastAPI(title="BYBOT.ai", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # em produção, coloque o domínio do seu front
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── modelos de request ─────────────────────────────────────────────────────────
class BotConfig(BaseModel):
    api_key: str
    api_secret: str
    symbol: str = "BTCUSDT"
    leverage: int = 5
    order_size_usdt: float = 100.0
    min_confidence: float = 68.0
    tp_pct: float = 1.5
    sl_pct: float = 0.8

class OrderRequest(BaseModel):
    api_key: str
    api_secret: str
    symbol: str
    side: str          # "Buy" ou "Sell"
    qty: float
    tp_price: Optional[float] = None
    sl_price: Optional[float] = None

class CloseRequest(BaseModel):
    api_key: str
    api_secret: str
    symbol: str
    side: str
    qty: float

# ── endpoints ──────────────────────────────────────────────────────────────────
_bot_task: Optional[asyncio.Task] = None

@app.post("/bot/start")
async def start_bot(cfg: BotConfig, background_tasks: BackgroundTasks):
    global _bot_task
    if state["bot_active"]:
        raise HTTPException(400, "Bot já está ativo")
    state["bot_active"] = True
    _bot_task = asyncio.create_task(bot_loop(cfg.dict()))
    return {"ok": True, "message": "Bot iniciado"}

@app.post("/bot/stop")
async def stop_bot():
    state["bot_active"] = False
    return {"ok": True, "message": "Bot encerrado"}

@app.get("/bot/status")
async def bot_status():
    return {
        "active": state["bot_active"],
        "open_position": state["open_position"],
        "last_signal": state["last_signal"],
        "balance": state["balance"],
        "pnl_today": state["pnl_today"],
        "orders": state["orders"][:20],
    }

@app.get("/market/ticker/{symbol}")
async def get_ticker(symbol: str, api_key: str, api_secret: str):
    client = BybitClient(api_key, api_secret)
    try:
        ticker = await client.get_ticker(symbol)
        return {"ok": True, "data": ticker}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/market/signal/{symbol}")
async def get_signal(symbol: str, api_key: str, api_secret: str):
    client = BybitClient(api_key, api_secret)
    try:
        klines = await client.get_klines(symbol, interval="5", limit=100)
        closes = [float(k[4]) for k in reversed(klines)]
        sig = compute_signal(closes)
        return {"ok": True, "signal": sig}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.post("/order/place")
async def place_order(req: OrderRequest):
    client = BybitClient(req.api_key, req.api_secret)
    try:
        result = await client.place_order(
            symbol=req.symbol,
            side=req.side,
            qty=req.qty,
            tp_price=req.tp_price,
            sl_price=req.sl_price,
        )
        if result.get("retCode") != 0:
            raise HTTPException(400, f"Bybit error: {result.get('retMsg')}")
        return {"ok": True, "data": result["result"]}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))

@app.post("/order/close")
async def close_position(req: CloseRequest):
    client = BybitClient(req.api_key, req.api_secret)
    try:
        result = await client.close_position(req.symbol, req.side, req.qty)
        state["open_position"] = None
        return {"ok": True, "data": result}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/account/balance")
async def get_balance(api_key: str, api_secret: str):
    client = BybitClient(api_key, api_secret)
    try:
        balance = await client.get_wallet()
        return {"ok": True, "balance": balance, "coin": "USDT"}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/account/positions")
async def get_positions(api_key: str, api_secret: str, symbol: str = "BTCUSDT"):
    client = BybitClient(api_key, api_secret)
    try:
        positions = await client.get_positions(symbol)
        return {"ok": True, "data": positions}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/health")
async def health():
    return {"status": "ok", "ts": datetime.now().isoformat()}
