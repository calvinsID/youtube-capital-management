from alpaca_trade_api.rest import REST

api = REST()

# Submit Order
api.submit_order (
    symbol = 'SPY',
    qty = 1.5,
    side = 'buy',
    type = 'market',
    time_in_force = 'gtc',
)
sample_order = {
    "id": "61e69015-8549-4bfd-b9c3-01e75843f47d",
    "client_order_id": "eb9e2aaa-f71a-4f51-b5b4-52a6c565dad4",
    "created_at": "2021-03-16T18:38:01.942282Z",
    "updated_at": "2021-03-16T18:38:01.942282Z",
    "submitted_at": "2021-03-16T18:38:01.937734Z",
    "filled_at": null,
    "expired_at": null,
    "canceled_at": null,
    "failed_at": null,
    "replaced_at": null,
    "replaced_by": null,
    "replaces": null,
    "asset_id": "b0b6dd9d-8b9b-48a9-ba46-b9d54906e415",
    "symbol": "AAPL",
    "asset_class": "us_equity",
    "notional": "500",
    "qty": null,
    "filled_qty": "0",
    "filled_avg_price": null,
    "order_class": "",
    "order_type": "market",
    "type": "market",
    "side": "buy",
    "time_in_force": "day",
    "limit_price": null,
    "stop_price": null,
    "status": "accepted",
    "extended_hours": False,
    "legs": null,
    "trail_percent": null,
    "trail_price": null,
    "hwm": null
}

# Show positions
api.list_positions()
sample_position = {
    "asset_id": "904837e3-3b76-47ec-b432-046db621571b",
    "symbol": "AAPL",
    "exchange": "NASDAQ",
    "asset_class": "us_equity",
    "avg_entry_price": "100.0",
    "qty": "5",
    "side": "long",
    "market_value": "600.0",
    "cost_basis": "500.0",
    "unrealized_pl": "100.0",
    "unrealized_plpc": "0.20",
    "unrealized_intraday_pl": "10.0",
    "unrealized_intraday_plpc": "0.0084",
    "current_price": "120.0",
    "lastday_price": "119.0",
    "change_today": "0.0084"
}

# Show asset
api.get_asset('AAPL')
sample_asset = {
    "id": "904837e3-3b76-47ec-b432-046db621571b",
    "class": "us_equity",
    "exchange": "NASDAQ",
    "symbol": "AAPL",
    "status": "active",
    "tradable": True,
    "marginable": True,
    "shortable": True,
    "easy_to_borrow": True,
    "fractionable": True
}

api.get_clock()
sample_clock = {
    "timestamp": "2018-04-01T12:00:00.000Z",
    "is_open": True,
    "next_open": "2018-04-01T12:00:00.000Z",
    "next_close": "2018-04-01T12:00:00.000Z"
}
