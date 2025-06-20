import os
import sys
import time
import requests
import threading
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
import math
import ccxt
import schedule
import traceback

from dotenv import load_dotenv
load_dotenv()

# Append the *parent* directory (main), not the db_config folder itself
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from db_config.db_config import Database


#---
# GET SERIOUS ABOUT LIFE
# PERSONAL DEVELOPMENT - GETTING TO BECOME THE KIND OF PERSON YOU WANT TO BE
# GET SMART (KNOWLEDGE, READ THE BOOKS, GET IDEA, WORK ON THEM)

stop_event = threading.Event()  # Global stop signal

API_URL = "https://medictreats.com/constra_api/save-trade.php"
UPDATE_API_URL = "https://medictreats.com/constra_api/update-trade.php"
TOKEN = os.getenv('EXTERNAL_API_TOKEN')

db_conn = Database(
    host= os.getenv('DB_HOST'),
    user= os.getenv('DB_USER'),
    password= os.getenv('DB_PASSWORD'),
    database= os.getenv('DB_DATABASE'),
    port= int(os.getenv('DB_PORT'))
)

def ensure_user_cred_table_exists():
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS user_cred (
        id INT AUTO_INCREMENT PRIMARY KEY,
        user_id INT NOT NULL,
        exchange_id INT NOT NULL,
        api_key VARCHAR(255) NOT NULL,
        secret VARCHAR(255) NOT NULL,
        password VARCHAR(200) DEFAULT NULL,
        status INT NOT NULL DEFAULT 1,
        date_added DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    """
    conn = db_conn.get_connection()
    if not conn:
        print("❌ No DB connection")
        return []
    with conn.cursor() as cursor:
        cursor.execute(create_table_sql)
    conn.commit()

def get_all_credentials_with_exchange_info():
    ensure_user_cred_table_exists()
    conn = db_conn.get_connection()
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT uc.id AS cred_id,
                   uc.api_key,
                   uc.secret,
                   uc.password,
                   uc.exchange_id,
                   ex.exchange_name,
                   ex.requirePass
            FROM user_cred uc
            JOIN exchanges ex ON uc.exchange_id = ex.id
            WHERE uc.status = 1
        """)
        return cursor.fetchall()

def fetch_trade_signals(user_cred_id, status):
    conn = db_conn.get_connection()
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT * FROM opn_trade
            WHERE user_cred_id = %s AND status = %s
        """, (user_cred_id, status))
        results = cursor.fetchall()
        return results if results else []

def insert_trade_signal(data):
    conn = db_conn.get_connection()
    try:
        with conn.cursor() as cursor:
            sql = """
                INSERT INTO opn_trade (user_cred_id, trade_signal, order_id, symbol, trade_type, amount, leverage, trail_threshold, profit_target_distance, trade_done, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (
                data['user_cred_id'],
                data['trade_signal'],
                data['order_id'],
                data['symbol'],
                data['trade_type'],
                data['amount'],
                data['leverage'],
                data['trail_threshold'],
                data['profit_target_distance'],
                data['trade_done'],
                data['status'],
            ))
            conn.commit()
            return True
            print(f"✅ Insert {data['symbol']} successful: (Take<-->Trade)")
    except Exception as e:
        print("❌ Insert failed:", e)
        return False
    finally:
        try:
            conn.close()
        except:
            pass  # ignore close error if conn was never set

def update_row(table_name, updates, conditions):
    """
    Updates rows in any table with flexible WHERE conditions and operators.

    :param table_name: Table name to update
    :param updates: Dict of column-value pairs to set
    :param conditions: Dict with values OR (operator, value) tuples for WHERE clause
    :return: True if any rows updated
    """
    if not updates or not conditions:
        raise ValueError("Both updates and conditions must be provided.")

    conn = db_conn.get_connection()
    with conn.cursor() as cursor:
        # SET clause
        set_clause = ', '.join([f"`{key}` = %s" for key in updates])
        update_values = list(updates.values())

        # WHERE clause with operator handling
        where_clauses = []
        where_values = []
        for key, val in conditions.items():
            if isinstance(val, tuple) and len(val) == 2:
                op, v = val
                where_clauses.append(f"`{key}` {op} %s")
                where_values.append(v)
            else:
                where_clauses.append(f"`{key}` = %s")
                where_values.append(val)

        where_clause = ' AND '.join(where_clauses)

        query = f"UPDATE `{table_name}` SET {set_clause} WHERE {where_clause}"
        cursor.execute(query, update_values + where_values)
        conn.commit()
        return cursor.rowcount > 0


def delete_row(table_name, conditions, log_table=None):
    """
    Deletes rows from any table with advanced WHERE conditions.
    Optionally logs the deleted rows to another table.

    :param table_name: Name of the table to delete from
    :param conditions: Dict of column: value, or column: (op, value), or column: ('IN', [list])
    :param log_table: Optional name of a table to insert deleted rows into before deletion
    :return: Number of rows deleted
    """
    if not conditions:
        raise ValueError("Conditions must be provided for safety.")

    conn = db_conn.get_connection()
    with conn.cursor() as cursor:
        # Build WHERE clause
        where_clauses = []
        where_values = []

        for col, val in conditions.items():
            if isinstance(val, tuple) and len(val) == 2:
                op, v = val
                if op.upper() == 'IN' and isinstance(v, list):
                    placeholders = ', '.join(['%s'] * len(v))
                    where_clauses.append(f"`{col}` IN ({placeholders})")
                    where_values.extend(v)
                else:
                    where_clauses.append(f"`{col}` {op} %s")
                    where_values.append(v)
            else:
                where_clauses.append(f"`{col}` = %s")
                where_values.append(val)

        where_clause = ' AND '.join(where_clauses)

        # Optional: Backup rows to log table
        if log_table:
            log_query = f"""
                INSERT INTO `{log_table}` SELECT * FROM `{table_name}` WHERE {where_clause}
            """
            cursor.execute(log_query, where_values)

        # Delete rows
        delete_query = f"DELETE FROM `{table_name}` WHERE {where_clause}"
        cursor.execute(delete_query, where_values)

        conn.commit()
        return cursor.rowcount > 0

def truncate_table(table_name):
    conn = db_conn.get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE `{table_name}`")
        conn.commit()
        print(f"✅ {table_name} table truncated (all rows cleared).")
        return True
    except Exception as e:
        print(f"❌ Failed to truncate table: {e}")
        return False

def save_trade_history(data: dict):
    try:
        response = requests.post(API_URL, json=data)
        print("🔍 RAW RESPONSE:", response.status_code)
        print("🔍 RESPONSE TEXT:", response.text)
        if response.status_code == 200:
            print("✅ Saved:", response.json())
            return True
        else:
            print("❌ Failed to save:", response.status_code, response.text)
            return False
    except Exception as e:
        print("⚠️ Error:", str(e))
        return False

def update_trade_history(data: dict):
    """
    Sends a flexible update request to your PHP backend.

    data = {
        "token": str,
        "table_name": str,
        "updates": dict,
        "conditions": dict
    }
    """
    try:
        response = requests.post(UPDATE_API_URL, json=data)
        print("🔄 RAW RESPONSE:", response.status_code)
        print("🔄 RESPONSE TEXT:", response.text)
        if response.status_code == 200:
            print("✅ Updated:", response.json())
            return True
        else:
            print("❌ Failed to update:", response.status_code, response.text)
            return False
    except Exception as e:
        print("⚠️ Error:", str(e))
        return False


def create_exchange(exchange_name, api_key, secret, password=None):
    """Create and return a CCXT exchange instance"""
    try:
        exchange_class = getattr(ccxt, exchange_name)
        config = {
            'apiKey': api_key,
            'secret': secret,
            'enableRateLimit': True,
            'options': {
                'defaultType': 'swap',
            },
        }

        # Add password if provided
        if password:
            config['password'] = password

        return exchange_class(config)
    except AttributeError:
        raise ValueError(f"Exchange '{exchange_name}' not found in CCXT")
    except Exception as e:
        raise Exception(f"Failed to create {exchange_name} exchange: {e}")


print_lock = threading.Lock()

def thread_safe_print(*args, **kwargs):
    with print_lock:
        print(*args, **kwargs)

def count_sig_digits(precision):
    # Count digits after decimal point if it's a fraction
    if precision < 1:
        return abs(int(round(math.log10(precision))))
    else:
        return 1  # Treat whole numbers like 1, 10, 100 as 1 sig digit

def round_to_sig_figs(num, sig_figs):
    if num == 0:
        return 0
    return round(num, sig_figs - int(math.floor(math.log10(abs(num)))) - 1)


def calculateLiquidationTargPrice(_liqprice, _entryprice, _percnt, _round):
    return round_to_sig_figs(_entryprice + (_liqprice - _entryprice) * _percnt, _round)

def reEnterTrade(exchange, symbol, order_side, order_price, order_amount, order_type):
    try:
        # Check if symbol is futures (adjust this check to your actual symbol format)
        if ":USDT" not in symbol:
            thread_safe_print(f"Skipping re-entry order for non-futures symbol: {symbol}")
            return

        # Fetch balance once
        balance_info = exchange.fetch_balance({'type': 'swap'})
        usdt_balance = balance_info.get('USDT', {}).get('free', 0)

        estimated_cost = order_amount * order_price

        # if usdt_balance < estimated_cost:
        #     thread_safe_print(f"⚠️ Insufficient USDT balance ({usdt_balance}) for order cost ({estimated_cost}). Skipping order.")
        #     return

        # First attempt: without posSide (works in one-way mode)
        order = exchange.create_order(
            symbol=symbol,
            type=order_type,
            side=order_side,
            amount=order_amount,
            price=order_price,
            params={
                'reduceOnly': False
            }
        )
        thread_safe_print(f"✅ Re-entry order placed: {order_side} {order_amount} @ {order_price}")

    except ccxt.BaseError as e:
        error_msg = str(e)
        # Handle specific phemex error for pilot contract
        if 'Pilot contract is not allowed here' in error_msg:
            thread_safe_print(f"❌ Phemex error: Pilot contract is not allowed for {symbol}. Skipping order.")
            return

        # If failed due to position mode, retry with posSide
        if 'TE_ERR_INCONSISTENT_POS_MODE' in error_msg:
            thread_safe_print("🔁 Retrying with (Limit) posSide due to inconsistent position mode...")
            pos_side = 'Long' if order_side == 'buy' else 'Short'
            try:
                order = exchange.create_order(
                    symbol=symbol,
                    type=order_type,
                    side=order_side,
                    amount=order_amount,
                    price=order_price,
                    params={
                        'reduceOnly': False,
                        'posSide': pos_side
                    }
                )
                thread_safe_print(f"✅ Re-entry Limit order (with posSide) placed: {order_side} {order_amount} @ {order_price}")
            except ccxt.BaseError as e2:
                thread_safe_print(f"❌ Re-entry Limit order failed even with posSide: {e2}")
        else:
            thread_safe_print(f"❌ Error placing re-entry Limit order: {e}")

def cancel_orphan_orders(exchange, symbol, side, order_type='limit'):
    """
    Cancel all open limit orders of the specified side for a symbol,
    assuming the position has already been closed.

    :param exchange: The ccxt exchange object
    :param symbol: Symbol string like 'BTCUSDT'
    :param side: 'buy' or 'sell' — from the original trade signal
    :param order_type: Default to 'limit'
    """
    try:
        open_orders = exchange.fetch_open_orders(symbol)
        if not open_orders:
            return  # No orders to cancel

        for order in open_orders:
            order_side = order['side'].lower()

            if order['type'] != order_type:
                continue

            if order_side != side.lower():
                continue  # Only cancel orders matching the passed-in side

            thread_safe_print(f"❌ Cancelling {order_side.upper()} {order_type.upper()} order for {symbol} (position closed)")

            try:
                exchange.cancel_order(order['id'], symbol)
            except Exception as e:
                if "TE_ERR_INCONSISTENT_POS_MODE" in str(e):
                    pos_side_str = "Long" if order_side == "buy" else "Short"
                    thread_safe_print(f"🔁 Retrying cancel with posSide={pos_side_str}")
                    exchange.cancel_order(order['id'], symbol, {'posSide': pos_side_str})
                else:
                    thread_safe_print(f"⚠️ Error cancelling order: {e}")

    except Exception as e:
        thread_safe_print(f"❌ Global error in cancel_orphan_orders: {e}")



def monitor_position_and_reenter(exchange, symbol, position, verbose=False, multiplier= 1.5):
    try:
        if not position:
            if verbose:
                thread_safe_print(f"No open position for {symbol}.")
            return

        # Extract critical values safely
        liquidation_price = float(position.get('liquidationPrice') or 0)
        entry_price = float(position.get('entryPrice') or 0)
        mark_price = float(position.get('markPrice') or 0)
        contracts = float(position.get('contracts') or 0)
        leverage = float(position.get("leverage") or 1)
        notional = float(position.get('notional') or 0)
        side = position.get('side', '').lower()

        if not (liquidation_price and entry_price and mark_price):
            return  # Skip if any key value is missing

        # Precision cache
        precision = exchange.markets[symbol]['precision']
        price_sig_digits = count_sig_digits(precision['price'])
        amount_sig_digits = count_sig_digits(precision['amount'])

        # Calculate proximity to liquidation
        distance_total = abs(entry_price - liquidation_price)
        distance_current = abs(mark_price - liquidation_price)
        closeness = 1 - (distance_current / distance_total) if distance_total else 0

        if verbose:
            thread_safe_print(f"[{symbol}] Side: {side}, Entry: {entry_price}, Mark: {mark_price}, "
                  f"Liquidation: {liquidation_price}, Closeness: {closeness*100:.1f}%")

        # Avoid re-entering if a same-side limit order already exists
        open_orders = exchange.fetchOpenOrders(symbol)
        same_side = 'buy' if side == 'long' else 'sell'
        if any(o['type'] == 'limit' and o['side'] == same_side for o in open_orders):
            if verbose:
                thread_safe_print(f"[{symbol}] Same-side limit order exists. Skipping re-entry.")
            return

        # Prepare re-entry order
        order_side = 'sell' if side == 'short' else 'buy'
        trigger_price = calculateLiquidationTargPrice(entry_price, liquidation_price, 0.2, price_sig_digits)

        # Double the notional for re-entry
        order_amount = round_to_sig_figs(((notional / leverage) * multiplier) / mark_price, amount_sig_digits)

        if verbose:
            thread_safe_print(f"[{symbol}] Re-entry Trigger: {trigger_price}, Amount: {order_amount}")

        reEnterTrade(exchange, symbol, order_side, trigger_price, contracts, 'limit')
        # Only re-enter if closeness is critical
        if closeness >= 0.8:
            if verbose:
                thread_safe_print(f"⚠️ Re-entry trigger initiated for {symbol}.")
        else:
            if verbose:
                thread_safe_print(f"✅ Not close enough for re-entry on {symbol}.")

    except ccxt.ExchangeError as e:
        thread_safe_print(f"Exchange error for {symbol}: {e}")
    except KeyError as ke:
        thread_safe_print(f"Missing key in {symbol} position data: {ke}")
    except Exception as e:
        thread_safe_print(f"Unexpected error in monitor_position_and_reenter for {symbol}: {e}")

def cancel_existing_stop_order(exchange, symbol, order_id, side):
    try:
        cancel_order = exchange.cancel_order(order_id, symbol=symbol)
        thread_safe_print(f"❌ Canceled previous stop-loss {order_id} (one-way) symol[{symbol}]")
        return True
    except Exception as e:
        if "TE_ERR_INCONSISTENT_POS_MODE" in str(e):
            try:
                params = {'posSide': 'Long' if side == 'long' else 'Short'}
                cancel_order = exchange.cancel_order(order_id, symbol=symbol, params=params)
                thread_safe_print(f"❌ Canceled stop-loss {order_id} (with posSide) symol[{symbol}]")
                return True
            except Exception as e2:
                thread_safe_print(f"⚠️ Still failed with posSide symol[{symbol}]: {e2}")
                return False
        else:
            thread_safe_print(f"⚠️ Cancel failed: {e}")
            return False

        # if cancel_order:
            # update_trail_order('')
    return False

def create_stop_order(exchange, symbol, side, contracts, new_stop_price):
    params_common = {
        'stopPx': new_stop_price,
        'triggerType': 'ByLastPrice',
        'triggerDirection': 1 if side == 'long' else 2,
        'reduceOnly': True,
        'closeOnTrigger': True,
        'timeInForce': 'GoodTillCancel',
    }

    # Try hedge mode first
    try:
        order = exchange.create_order(
            symbol=symbol,
            type='stop',
            side='sell' if side == 'long' else 'buy',
            amount=contracts,
            price=None,
            params={**params_common, 'positionIdx': 1 if side == 'long' else 2, 'posSide': 'Long' if side == 'long' else 'Short'}
        )
        thread_safe_print(f"✅ Stop-loss set at {new_stop_price:.4f} (hedge mode)")
        return order
    except Exception as e:
        thread_safe_print(f"⚠️ Hedge mode failed: {e}")

    # Fallback to one-way mode
    try:
        order = exchange.create_order(
            symbol=symbol,
            type='stop',
            side='sell' if side == 'long' else 'buy',
            amount=contracts,
            price=None,
            params=params_common
        )
        thread_safe_print(f"✅ Stop-loss set at {new_stop_price:.4f} (one-way mode)")
        return order
    except Exception as e2:
        thread_safe_print(f"❌ Both order attempts failed: {e2}")
        return None

def set_phemex_leverage(exchange, symbol, leverage=None, long_leverage=None, short_leverage=None):
    clean_symbol = symbol.split(':')[0].replace('/', '')  # BIDUSDT format
    
    path = 'g-positions/leverage'
    method = 'PUT'
    
    # Compose query params as strings (required by API)
    params = {
        'symbol': clean_symbol,
    }
    
    if leverage is not None:
        params['leverageRr'] = str(leverage)  # One-way mode leverage
    
    if long_leverage is not None and short_leverage is not None:
        params['longLeverageRr'] = str(long_leverage)
        params['shortLeverageRr'] = str(short_leverage)
    
    try:
        response = exchange.fetch2(path, 'private', method, params)
        print(f"Set leverage response: {response}")
    except Exception as e:
        print(f"⚠️ Could not set leverage: {e}")

def trailing_stop_logic(exchange, position, trade_id, trade_order_id, trail_order_id, trail_theshold, profit_target_distance, breath_stop, breath_threshold):
    symbol = position.get('symbol')
    entry_price = float(position.get('entryPrice') or 0)
    mark_price = float(position.get('markPrice') or 0)
    sideRl = position.get('side', '')
    side = sideRl.lower()
    leverage = float(position.get("leverage") or 1)
    leverageDefault = 5
    contracts = float(position.get('contracts') or 0)
    margin_mode = position.get('marginMode') or position['info'].get('marginType')

    if not entry_price or not mark_price or side not in ['long', 'short'] or contracts <= 0:
        return
    if margin_mode != "isolated":
        pos_mode = position['info'].get('posMode', '').lower()
        print(f"🛑🛑🛑🛑Symbol: [{symbol}] side: {sideRl}, posMode: {pos_mode} | posSide: {position['info'].get('posSide', '')}")
        if pos_mode == 'oneway':
            set_phemex_leverage(exchange, symbol, leverage=leverageDefault)
        elif pos_mode == 'hedged':
            set_phemex_leverage(exchange, symbol, long_leverage=leverageDefault, short_leverage=leverageDefault)

    if leverage != leverageDefault:
        # Depending on mode, set leverage appropriately as above
        pos_mode = position['info'].get('posMode', '').lower()
        print(f"🛑🛑Symbol: [{symbol}] side: {sideRl}, posMode: {pos_mode} | posSide: {position['info'].get('posSide', '')}")
        if pos_mode == 'oneway':
            set_phemex_leverage(exchange, symbol, leverage=leverageDefault)
        elif pos_mode == 'hedged':
            set_phemex_leverage(exchange, symbol, long_leverage=leverageDefault, short_leverage=leverageDefault)

    change = (mark_price - entry_price) / entry_price if side == 'long' else (entry_price - mark_price) / entry_price
    profit_distance = change * leverage
    unrealized_pnl = (mark_price - entry_price) * contracts if side == 'long' else (entry_price - mark_price) * contracts
    realized_pnl = float(position["info"].get('curTermRealisedPnlRv') or 0)
    total_pnl = unrealized_pnl + realized_pnl

    thread_safe_print(f"\n📈💰 {symbol} ({side.upper()}) | Leverage: {leverage} | Contract(Amount): {contracts} | MarginMode: {margin_mode}")
    thread_safe_print(f"Profit-Distance: {profit_distance}, PNL → Unrealized: {unrealized_pnl:.4f}, Realized: {realized_pnl:.4f}, Total: {total_pnl:.4f}")

    if total_pnl <= 0.001:
        if trail_order_id:
            cancel_conditional_order = cancel_existing_stop_order(exchange, symbol, trail_order_id, side)
            if cancel_conditional_order:
                trailing_update = update_row(
                    table_name = 'opn_trade',
                    updates = {
                        'trail_order_id': "",
                        'trail_threshold': trail_theshold,
                        'profit_target_distance': profit_target_distance,
                        'trade_done': 0
                    },
                    conditions = {'id': ('=', trade_id),'symbol': symbol})
                if trailing_update:
                    print("No more trailing (for now) - Trade Order Id: ", trade_order_id)
                    update_trade_history({
                        "token": TOKEN,
                        "table_name": "trade_history",
                        "updates": {
                            "trail_threshold": trail_theshold,
                            "profit_target_distance": profit_target_distance,
                            "trade_done": 0
                        },
                        "conditions": {
                            "order_id": ("=", trade_order_id),
                            "symbol": symbol
                        }
                    })
        return

    if profit_distance >= trail_theshold:
        new_stop_price = entry_price * (1 + profit_target_distance / leverage) if side == 'long' else entry_price * (1 - profit_target_distance / leverage)
        if (side == 'long' and new_stop_price <= entry_price) or (side == 'short' and new_stop_price >= entry_price):
            thread_safe_print(f"❌ Invalid stop-loss price {new_stop_price:.4f} vs entry {entry_price}")
            return

        if trail_order_id:
            cancel_conditional_order = cancel_existing_stop_order(exchange, symbol, trail_order_id, side)
            if cancel_conditional_order:
                print(f"Removeing Old conditional🤗🤗: {symbol} -> {trail_order_id}")
                
        order = create_stop_order(exchange, symbol, side, contracts, new_stop_price)
        if order:
            new_trail_order_id = order['id']
            new_trail_theshold = trail_theshold + breath_threshold
            new_profit_target_distance = profit_target_distance + breath_stop
            trailing_update = update_row(
                table_name = 'opn_trade',
                updates = {
                    'trail_order_id': new_trail_order_id,
                    'trail_threshold': new_trail_theshold,
                    'profit_target_distance': new_profit_target_distance,
                    'trade_done': 1
                },
                conditions = {
                    'id': ('=', trade_id),
                    'symbol': symbol
                }
            )
            if trailing_update:
                print("Trade Order Id: ", trade_order_id)
                update_trade_history({
                    "token": TOKEN,
                    "table_name": "trade_history",
                    "updates": {
                        "trail_threshold": new_trail_theshold,
                        "profit_target_distance": new_profit_target_distance,
                        "trade_done": 1
                    },
                    "conditions": {
                        "order_id": ("=", trade_order_id),
                        "symbol": symbol
                    }
                })
            # update_trailing_data(trade_id, symbol, new_trail_order_id, new_trail_theshold, new_profit_target_distance, 1)
            # save_trailing_data(symbol, trailing_data, side)

def mark_trade_signal_closed_if_position_closed(exchange, symbol, trade_order_id, trade_id, side, positionst):
    """
    Checks if a position with the given side is still open for the symbol.
    If not, updates trade_signal.status = 0 for that trade_id.

    :param symbol: Symbol like 'BTCUSDT'
    :param trade_id: The ID of the trade_signal row
    :param side: 'buy' or 'sell' from trade signal
    :param positionst: List of positions from exchange
    """
    target_side = 'long' if side.lower() == 'buy' else 'short'

    is_open = any(
        pos['symbol'] == symbol and
        pos.get('contracts', 0) > 0 and
        pos.get('side') == target_side
        for pos in positionst
    )

    if not is_open:
        backup_trade_final = update_trade_history ({
            "token": TOKEN,
            "table_name":"trade_history",
            "updates":{'status': 0},
            "conditions":{'order_id': trade_order_id}
        })

        if backup_trade_final:
            thread_safe_print(f"🔄 Symbol {symbol} [{side.upper()}] closed. Marked trade_signal ID {trade_id} as status=0.")
            cancel_orphan_orders(exchange, symbol, side, 'limit')
            is_deleted = delete_row(
                table_name='opn_trade',
                conditions={'id': trade_id}
            )
            if is_deleted:
                 print(f"✅ Trade: {trade_order_id} deleted.")
            else:
                print(f"⚠️ Trade: {trade_order_id} deleted.")
        else:
            thread_safe_print(f"An Error occured while for {symbol} [{side.upper()}]. Marked trade_signal ID {trade_id} as status=0")
            thread_safe_print(f"Retrying [Deleting trade {trade_order_id}]....")
            cancel_orphan_orders(exchange, symbol, side, 'limit')
            is_deleted = delete_row(
                table_name='opn_trade',
                conditions={'id': trade_id}
            )
            if is_deleted:
                 thread_safe_print(f"✅ Trade: {trade_order_id} deleted.")
            else:
                thread_safe_print(f"⚠️ Trade: {trade_order_id} deleted.")
                
def fetch_open_usdt_positions(exchange):
    try:
        response = exchange.fetch2('g-accounts/accountPositions', 'private', 'GET', {'currency': 'USDT'})
    except Exception:
        response = exchange.fetch2('accounts/positions', 'private', 'GET', {'currency': 'USDT'})

    all_positions = response.get('data', {}).get('positions', [])
    # Filter only open positions
    open_positions = [
        pos for pos in all_positions
        if float(pos.get('size', 0)) > 0
    ]
    return open_positions

# We try to find the matching market symbol that corresponds to this raw symbol
def find_matching_symbol(raw_symbol, markets):
    # raw_symbol is like 'u1000RATSUSDT'
    # try to match to any market where info['symbol'] matches raw_symbol
    for market_symbol, market in markets.items():
        info_symbol = market['info'].get('symbol', '')
        if info_symbol == raw_symbol:
            return market_symbol
    return None
    
        
def sync_open_orders_to_db(exchange, user_id):
    """
    Sync actual executed market orders (not pending limit/market) to DB for given user_id.
    """
    try:
        markets = exchange.load_markets()
        # Check if this order already exists in DB
        conn = db_conn.get_connection()
        cursor = conn.cursor()
        positions = fetch_open_usdt_positions(exchange)
        for position in positions:
            # print("Position: ", position)
            raw_pos_symbol = position['symbol']
            side = position['side'].lower()
            symbol = find_matching_symbol(raw_pos_symbol, markets)
            if not symbol:
                print(f"No matching ccxt market symbol found for raw position symbol {raw_pos_symbol}")
            contracts = float(position.get('contracts', 0))
            matching_order_id = f"{user_id}_{symbol}_live_{side}"

            cursor.execute(
                "SELECT 1 FROM opn_trade WHERE order_id=%s AND symbol=%s AND user_cred_id=%s AND status = 1 LIMIT 1",
                (matching_order_id, symbol, user_id)
            )
            if cursor.fetchone():
                continue  # Already stored
            side = position['side'].lower()
            # print(f"Symbol: {symbol} and side: {side}")
            side_int = 0 if side == 'buy' else 1 if side == 'sell' else None
            trail_thresh = 0.10  # 10%
            profit_target_distance = 0.06  # 6%

            trade_data = {
                "user_cred_id": user_id,
                "trade_signal": -10,
                "order_id": matching_order_id,
                "symbol": symbol,
                "trade_type": side_int,
                "amount": float(position.get('size', 0)),
                "leverage": float(position.get('leverageRr', 1)),
                "trail_threshold": trail_thresh,
                "profit_target_distance": profit_target_distance,
                "trade_done": 0,
                "status": 1
            }

            if insert_trade_signal(trade_data):
                print(f"✅ Inserted trade for {symbol} [order_id: {matching_order_id}]")

                backup_data = {
                    "token": TOKEN,
                    "user_id": user_id,
                    "order_id": matching_order_id,
                    "symbol": symbol,
                    "trade_type": side_int,
                    "amount": float(position.get('size', 0)),
                    "leverage": float(position.get('leverageRr', 1)),
                    "trail_threshold": trail_thresh,
                    "profit_target_distance": profit_target_distance,
                    "trade_done": 0,
                    "status": 1
                }

                if save_trade_history(backup_data):
                    print(f"☁️ Backup complete for {symbol} [order_id: {matching_order_id}]")
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"❌ Error syncing symbol {symbol}: {e}")

def process_single_position(exchange, pos, signal_map, positionst):
    symbol = pos['symbol']
    row = signal_map.get(symbol, {})

    # Default fallbacks if values are missing
    trade_id = row.get('id')
    trade_order_id = row.get('order_id')
    trail_order_id = row.get('trail_order_id')
    trail_thresh = float(row.get('trail_threshold', 0.10))
    trail_profit_distance = float(row.get('profit_target_distance', 0.06))
    side_int = row.get('trade_type')
    trade_done = row.get('trade_done')
    status = row.get('status')

    side = 'buy' if side_int == 0 else 'sell' if side_int == 1 else None

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = []
            if pos.get('contracts', 0) > 0:
                futures.append(executor.submit(
                    trailing_stop_logic, exchange, pos, trade_id, trade_order_id,
                    trail_order_id, trail_thresh, trail_profit_distance, 0.10, 0.10
                ))
                futures.append(executor.submit(
                    monitor_position_and_reenter, exchange, symbol, pos, True
                ))
            else:
                futures.append(executor.submit(
                    mark_trade_signal_closed_if_position_closed,
                    exchange, symbol, trade_order_id, trade_id, side, positionst
                ))

            # Wait for all tasks to finish and catch exceptions
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                    time.sleep(1)  # short pause to prevent busy loop
                except Exception as e:
                    thread_safe_print(f"❌ Error in async task for symbol {symbol}: {e}")
                    traceback.print_exc()
        thread_safe_print(f"--------------🙌 Position processed for {symbol} 🙌---------------")
    except Exception as e:
        thread_safe_print(f"❌ Error processing position for symbol {symbol}: {e}")
        traceback.print_exc()

def main_job(exchange, user_cred_id, verify):
    try:
        trade_signals = fetch_trade_signals(user_cred_id=user_cred_id, status=1)

        if not trade_signals:
            thread_safe_print(f"[{exchange.apiKey[:6]}...] ⚠️ No trade signals found.")
            return

        signal_map = {row['symbol']: row for row in trade_signals}
        symbols = list(signal_map.keys())

        positionst = exchange.fetch_positions(symbols=symbols)
        usdt_balances = exchange.fetch_balance({'type': 'swap'}).get('USDT', {})
        usdt_balance_free = usdt_balances.get('free', 0)
        usdt_balance_total = usdt_balances.get('total', 0)
        thread_safe_print(f"[{exchange.apiKey[:6]}...] USDT Balance->Free: {usdt_balance_free}")
        thread_safe_print(f"[{exchange.apiKey[:6]}...] USDT Balance->Total: {usdt_balance_total}")

        # Use ThreadPoolExecutor to process all positions concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(process_single_position, exchange, pos, signal_map, positionst)
                for pos in positionst
            ]

            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    thread_safe_print(f"❌ Error in processing a position in main_job: {e}")
                    traceback.print_exc()

    except Exception:
        thread_safe_print(f"❌ Error for exchange [{exchange.apiKey[:6]}...]:")
        traceback.print_exc()
    
def run_exchanges_in_batch(batch):
    # clear_table = truncate_table("opn_trade")
    while not stop_event.is_set():
        for item in batch:
            if len(item) != 3:
                thread_safe_print(f"⚠️ Unexpected tuple size: {item}")
                continue
            exchange_obj, user_cred_id, verify = item
            try:
                # Run main job synchronously (quick, non-blocking)
                main_job(exchange=exchange_obj, user_cred_id=user_cred_id, verify=verify)
            except Exception:
                thread_safe_print(f"❌ Error in main_job for user {user_cred_id}")
                traceback.print_exc()

        time.sleep(3)  # short pause to prevent busy loop


def sync_open_orders_loop_batch(batch):
    cooldown_seconds = 1 * 60  # 30 minutes
    last_sync_times = { (ex.id, user_id): 0 for ex, user_id, _ in batch }

    while not stop_event.is_set():
        current_time = time.time()
        for exchange, user_id, _ in batch:
            key = (exchange.id, user_id)
            if current_time - last_sync_times[key] >= cooldown_seconds:
                try:
                    sync_open_orders_to_db(exchange, user_id)
                    last_sync_times[key] = current_time
                except Exception as e:
                    print(f"❌ Error syncing user {user_id} on {exchange.id}: {e}")
        # Sleep a bit to avoid tight loop
        time.sleep(min(2, cooldown_seconds / 2))  # e.g. 2 seconds


def run_all():
    credentials = get_all_credentials_with_exchange_info()  # JOINed data
    if not credentials:
        thread_safe_print("⚠️ No API credentials found in the database. Exiting...")
        return

    exchange_list = []
    for row in credentials:
        try:
            exchange_name = row['exchange_name']
            requires_password = row['requirePass']
            password = row['password'] if requires_password != 0 else None
            verify = row['api_key'][:6]
            exchange = create_exchange(exchange_name, row['api_key'], row['secret'], password)
             # ✅ Preload markets to reduce per-thread overhead
            exchange.load_markets()
            exchange.options['warnOnFetchOpenOrdersWithoutSymbol'] = False
            exchange_list.append((exchange, row['cred_id'], verify))
        except Exception as e:
            print(row)
            thread_safe_print(f"❌ Failed to create exchange for API key {row['api_key'][:6]}...: {e}")

    if not exchange_list:
        thread_safe_print("⚠️ No valid exchanges could be created. Exiting...")
        return

    total = len(exchange_list)
    batch_size = max(1, int(math.sqrt(total)))  # √N batching for load balancing
    thread_safe_print(f"Total Accounts: {total}, Batch size: {batch_size}")

    batches = [exchange_list[i:i + batch_size] for i in range(0, total, batch_size)]

    threads = []
    for batch in batches:
        # Start main job thread for batch
        t = threading.Thread(target=run_exchanges_in_batch, args=(batch,), daemon=True)
        t.start()
        threads.append(t)

        # Start one sync thread per batch, handling all exchange-users in it
        sync_t = threading.Thread(target=sync_open_orders_loop_batch, args=(batch,), daemon=True)
        sync_t.start()
        threads.append(sync_t)

    try:
        while True:
            time.sleep(0.8)
    except KeyboardInterrupt:
        thread_safe_print("\n🛑 Ctrl+C detected. Stopping all threads...")
        stop_event.set()
        time.sleep(2)

if __name__ == "__main__":
    run_all()
