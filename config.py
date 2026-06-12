from pydantic_settings import BaseSettings
from typing import List


class Settings(BaseSettings):
    binance_api_key: str = ""
    binance_secret_key: str = ""

    trading_mode: str = "paper"  # "paper" or "live"
    testnet: bool = False  # use binance testnet

    min_volume_24h_usdt: float = 1_000_000
    max_symbols: int = 100
    min_spread_pct: float = 0.001
    min_funding_rate_abs: float = 0.00001

    taker_fee: float = 0.001
    maker_fee: float = 0.001

    paper_initial_balance: float = 10_000.0
    max_position_size_usdt: float = 1_000.0

    auto_trade_enabled: bool = False
    trade_size_pct: float = 10.0
    min_net_profit_usdt: float = 0.5
    slippage_pct: float = 0.05
    max_concurrent_positions: int = 5
    cooldown_seconds: int = 60

    ui_host: str = "127.0.0.1"
    ui_port: int = 8000
    ui_reload: bool = False

    db_path: str = "arbitrage.db"

    update_interval_ms: int = 100

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
