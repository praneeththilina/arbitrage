import asyncio

from config import settings
from exchange.client import TickerData
from strategies.funding_arb import FundingArbitrage
from strategies.basis_arb import BasisArbitrage
from strategies.triangular_arb import TriangularArbitrage
from ui import server


class DummyClient:
    def __init__(self):
        self.tickers = {}


async def _run_scanner_once(scanner):
    original_interval = settings.update_interval_ms
    settings.update_interval_ms = 1
    task = asyncio.create_task(scanner.start())
    await asyncio.sleep(0)

    try:
        assert scanner.is_running is True
        await scanner.start()
        assert scanner.is_running is True
    finally:
        scanner.stop()
        await asyncio.wait_for(task, timeout=1)
        settings.update_interval_ms = original_interval

    assert scanner.is_running is False


def test_funding_scanner_ignores_duplicate_starts_and_stops_cleanly():
    asyncio.run(_run_scanner_once(FundingArbitrage(DummyClient())))


def test_basis_scanner_ignores_duplicate_starts_and_stops_cleanly():
    asyncio.run(_run_scanner_once(BasisArbitrage(DummyClient())))


def test_triangular_scanner_ignores_duplicate_starts_and_stops_cleanly():
    asyncio.run(_run_scanner_once(TriangularArbitrage(DummyClient())))


def test_basis_scanner_scores_spot_futures_premium():
    client = DummyClient()
    client.tickers["BTCUSDT"] = TickerData(
        spot_price=100.0,
        futures_price=101.0,
        bid=99.95,
        ask=100.05,
        spot_volume_24h=10_000_000,
        futures_volume_24h=12_000_000,
    )

    op = BasisArbitrage(client)._evaluate("BTCUSDT", client.tickers["BTCUSDT"])

    assert op is not None
    assert op.symbol == "BTCUSDT"
    assert op.action == "short_perp_long_spot"
    assert op.basis_pct > 0
    assert op.expected_profit_pct > 0


class FakeStrategy:
    def __init__(self):
        self.is_running = False
        self.stop_called = False

    async def start(self):
        self.is_running = True
        try:
            while self.is_running:
                await asyncio.sleep(0.01)
        finally:
            self.is_running = False

    def stop(self):
        self.stop_called = True
        self.is_running = False


async def _exercise_server_strategy_lifecycle():
    original_funding = server.funding_strategy
    original_funding_task = server._funding_task
    original_basis = server.basis_strategy
    original_basis_task = server._basis_task
    original_triangular = server.triangular_strategy
    original_triangular_task = server._triangular_task

    fake = FakeStrategy()
    server.funding_strategy = fake
    server._funding_task = None
    server.basis_strategy = None
    server._basis_task = None
    server.triangular_strategy = None
    server._triangular_task = None

    try:
        started = await server.start_strategy("funding")
        assert started["status"] == "started"
        assert started["strategies"]["funding"]["task_active"] is True

        duplicate = await server.start_strategy("funding")
        assert duplicate["status"] == "already_running"

        stopped = await server.stop_strategy("funding")
        assert stopped["status"] == "stopped"
        assert stopped["strategies"]["funding"]["task_active"] is False
        assert fake.stop_called is True
        await asyncio.sleep(0)
    finally:
        if server._funding_task and not server._funding_task.done():
            server._funding_task.cancel()
            try:
                await server._funding_task
            except asyncio.CancelledError:
                pass
        server.funding_strategy = original_funding
        server._funding_task = original_funding_task
        server.basis_strategy = original_basis
        server._basis_task = original_basis_task
        server.triangular_strategy = original_triangular
        server._triangular_task = original_triangular_task


def test_server_rejects_duplicate_strategy_tasks_and_stops_active_task():
    asyncio.run(_exercise_server_strategy_lifecycle())


async def _exercise_server_basis_lifecycle():
    original_basis = server.basis_strategy
    original_basis_task = server._basis_task

    fake = FakeStrategy()
    server.basis_strategy = fake
    server._basis_task = None

    try:
        started = await server.start_strategy("basis")
        assert started["status"] == "started"
        assert started["strategies"]["basis"]["task_active"] is True

        duplicate = await server.start_strategy("basis")
        assert duplicate["status"] == "already_running"

        stopped = await server.stop_strategy("basis")
        assert stopped["status"] == "stopped"
        assert stopped["strategies"]["basis"]["task_active"] is False
        assert fake.stop_called is True
        await asyncio.sleep(0)
    finally:
        if server._basis_task and not server._basis_task.done():
            server._basis_task.cancel()
            try:
                await server._basis_task
            except asyncio.CancelledError:
                pass
        server.basis_strategy = original_basis
        server._basis_task = original_basis_task


def test_server_rejects_duplicate_basis_tasks_and_stops_active_task():
    asyncio.run(_exercise_server_basis_lifecycle())
