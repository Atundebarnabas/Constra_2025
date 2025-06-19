import os
import sys
import requests
import threading
import time
import math
import json
import ccxt
import pandas as pd
import schedule
import traceback
from dotenv import load_dotenv
load_dotenv()

# Append the *parent* directory (main), not the db_config folder itself
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from db_config.db_config import Database

print_lock = threading.Lock()

API_URL = "https://medictreats.com/constra_api/save-trade.php"



#|||||||||||||||||||||||||||||#
#   DATABASE CONFIGURATION    #
#|||||||||||||||||||||||||||||#
    
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
    with conn.cursor() as cursor:
        cursor.execute(create_table_sql)
    conn.commit()
    
def ensure_opn_trade_table_exists():
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS opn_trade (
        id INT AUTO_INCREMENT PRIMARY KEY,
        user_cred_id INT NOT NULL,
        trade_signal INT NOT NULL,
        order_id VARCHAR(255) NOT NULL,
        symbol VARCHAR(255) NOT NULL,
        trade_type INT NOT NULL,
        amount DOUBLE(16, 3) DEFAULT 0.000,
        leverage INT NOT NULL,
        trail_order_id VARCHAR(255),
        trail_threshold DOUBLE(16, 4) DEFAULT 0.0000,
        profit_target_distance DOUBLE(16, 4) DEFAULT 0.0000,
        to_liquidation_order_id VARCHAR(255),
        trade_done TINYINT DEFAULT 0,
        status TINYINT NOT NULL DEFAULT 1,
        date_added DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    """
    try:
        conn = db_conn.get_connection()
        with conn.cursor() as cursor:
            cursor.execute(create_table_sql)
        conn.commit()
    except Exception as e:
        print(f"❌ Failed to create table: {e}")
    finally:
        conn.close()
  
def get_all_credentials_with_exchange_info():
    ensure_user_cred_table_exists()
    ensure_opn_trade_table_exists()
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



def ensure_exchanges_table_exists():
  create_table_sql = """
  CREATE TABLE IF NOT EXISTS exchanges (
      id INT AUTO_INCREMENT PRIMARY KEY,
      exchange_name VARCHAR(255) NOT NULL,
      requirePass INT NOT NULL DEFAULT 0,
      status INT NOT NULL DEFAULT 1,
      date_added DATETIME DEFAULT CURRENT_TIMESTAMP
  );
  """
  conn = db_conn.get_connection()
  with conn.cursor() as cursor:
      cursor.execute(create_table_sql)
  conn.commit()
  
  
def get_exchange_by_id(id):
  ensure_exchanges_table_exists()
  conn = db_conn.get_connection()
  with conn.cursor() as cursor:
      cursor.execute(f"SELECT * FROM exchanges WHERE id = {id} AND status = 1")
      return cursor.fetchall()
  
def fetch_single_trade_signal():
    conn = db_conn.get_connection()
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT * FROM trade_signal 
            WHERE status = 1 
            ORDER BY RAND() + UNIX_TIMESTAMP(date_added) / 1000000 DESC 
            LIMIT 1
        """)
        return cursor.fetchone()


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
      
def has_open_trade(user_cred_id, symbol):
    try:
        conn = db_conn.get_connection()
        with conn.cursor() as cursor:
            query = """
                SELECT 1 FROM opn_trade
                WHERE user_cred_id = %s AND symbol = %s AND status = 1
                LIMIT 1
            """
            cursor.execute(query, (user_cred_id, symbol))
            result = cursor.fetchone()
            return result is not None
    except Exception as e:
        print(f"Error checking trade: {e}")
        return False

def get_side_count(user_cred_id, trade_done, side):
    try:
        conn = db_conn.get_connection()
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) AS count FROM opn_trade
                WHERE user_cred_id = %s AND trade_type = %s AND trade_done = %s AND status = 1
            """, (user_cred_id, trade_done, side))
            result = cursor.fetchone()
            return result['count']
    except Exception as e:
        print(f"Error Fetching trade side count for User Cred ID: {user_cred_id} \nError: {e}")
        return False


      
def clear_trade_signals_for_exchange(exchange_db_id):
  try:
      with api_db_conn.cursor() as cursor:
          cursor.execute("DELETE FROM trade_signal WHERE exchange = %s", (exchange_db_id,))
      api_db_conn.commit()
      print(f"✅ Cleared trade signals for exchange ID {exchange_db_id}")
  except Exception as e:
      print(f"❌ Failed to clear trade signals: {e}")

          
def drop_table(table_name):
  conn = db_conn.get_connection()
  try:
      with conn.cursor() as cursor:
          cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
      conn.commit()
      print(f"✅ {table_name} table deleted.")
  except Exception as e:
      print(f"❌ Failed to drop table: {e}")

def create_exchange(exchange_name, api_key, secret, password=None):
    """Create and return a CCXT exchange instance"""
    try:
        exchange_class = getattr(ccxt, exchange_name)
        config = {
            'apiKey': api_key,
            'secret': secret,
            'enableRateLimit': True
        }
        
        # Add password if provided
        if password:
            config['password'] = password
            
        return exchange_class(config)
    except AttributeError:
        raise ValueError(f"Exchange '{exchange_name}' not found in CCXT")
    except Exception as e:
        raise Exception(f"Failed to create {exchange_name} exchange: {e}")
    
def save_trade_history(data: dict):
    try:
        response = requests.post(API_URL, json=data)
        print("🔍 RAW RESPONSE:", response.status_code)
        print("🔍 RESPONSE TEXT:", response.text)
        if response.status_code == 200:
            print("✅ Saved:", response.json())
        else:
            print("❌ Failed to save:", response.status_code, response.text)
    except Exception as e:
        print("⚠️ Error:", str(e))


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

def has_open_position(exchange, symbol):
    try:
        positions = exchange.fetch_positions([symbol])  # Only for the given symbol
        for pos in positions:
            if pos['symbol'] == symbol and float(pos.get('contracts', 0)) > 0:
                return True
        return False
    except Exception as e:
        print(f"❌ Error checking live position for {symbol}: {e}")
        return False

def generate_mixed_token(exchange_name: str, secret: str, length: int = 24):
    combined = ""
    min_len = min(len(exchange_name), len(secret))

    # Interleave exchange_name and secret characters
    for i in range(min_len):
        combined += exchange_name[i] + secret[i]

    # If we still need more characters
    combined += secret[min_len:min_len + (length - len(combined))]

    return combined[:length]



# Helper function to get open long/short counts
def get_open_position_counts(exchange, all_symbols):
    positions = exchange.fetch_positions(symbols=all_symbols)
    open_positions = [pos for pos in positions if pos.get('contracts') and abs(float(pos['contracts'])) > 0]
    short_positions = [
        pos for pos in open_positions
        if (pos.get('side') == 'short') or
        ('size' in pos and float(pos['size']) < 0) or
        ('info' in pos and pos['info'].get('side', '').lower() == 'sell')
    ]
    long_positions = [
        pos for pos in open_positions
        if (pos.get('side') == 'long') or
        ('size' in pos and float(pos['size']) > 0) or
        ('info' in pos and pos['info'].get('side', '').lower() == 'buy')
    ]
    return open_positions, len(short_positions), len(long_positions)

def issueNumberOfTrade(acc_bal):
    thresholds = [
        (10000, 20),
        (5000, 17),
        (1000, 15),
        (500, 12),
        (150, 10),
        (0, 8)
    ]
    
    for limit, trades in thresholds:
        if acc_bal >= limit:
            return trades

    

def calculateIntialAmount(account_balance, leverage= 5, divider= 30.0):
    MAX_NUMBER_TRADE = issueNumberOfTrade(account_balance)
    FIRST_ENTRY = round_to_sig_figs((account_balance / divider), 2)
    FIRST_ENTRY_PER_TRADE = round_to_sig_figs((FIRST_ENTRY / MAX_NUMBER_TRADE), 2)
    INITIAL_AMOUNT = FIRST_ENTRY_PER_TRADE * leverage
    return FIRST_ENTRY_PER_TRADE

# Trading parameters
leverage = 5
multiplier= 2
fromPercnt = 0.2  #20%

def calculateLiquidationTargPrice(_liqprice, _entryprice, _percnt, _round):
    return round_to_sig_figs(_entryprice + (_liqprice - _entryprice) * _percnt, _round)

def get_base_amount(exchange, symbol, usdt_value):
    ticker = exchange.fetch_ticker(symbol)
    market_price = ticker['last']
    base_amount = usdt_value / market_price
    return base_amount

def place_entry_and_liquidation_limit_order (exchange, symbol, side, usdt_value, leverage):
    try:
        # 🟡 Fetch current market price
        ticker = exchange.fetch_ticker(symbol)
        market_price = ticker['last']
        base_amount = usdt_value / market_price
        pos_side = 'Long' if side == 'buy' else 'Short'

        # 🟡 Set isolated margin
        try:
            exchange.set_margin_mode('isolated', symbol)
        except Exception as e:
            thread_safe_print(f"⚠️ Could not set isolated margin for {symbol}: {e}")
            try:
                exchange.set_margin_mode('isolated', symbol, params={'posSide': pos_side})
                thread_safe_print(f"Successfully set margin mode with posSide={pos_side}")
            except Exception as e2:
                thread_safe_print(f"Failed again with posSide: {e2}")

        # 🟡 Set leverage
        try:
            exchange.set_leverage(leverage, symbol)
        except Exception as e:
            thread_safe_print(f"⚠️ Could not set leverage for {symbol}: {e}")
            try:
                exchange.set_leverage(leverage, symbol, params={'posSide': pos_side})
                thread_safe_print(f"Successfully set leverage with posSide={pos_side}")
            except Exception as e2:
                thread_safe_print(f"Failed again with posSide: {e2}")

        thread_safe_print(f"🔔 ORDER → {symbol} | {side.upper()} | Price: {market_price:.4f} | Qty: {base_amount:.5f}")

        # 🟢 Place market order (with fallback to posSide)
        try:
            market_order = exchange.create_order(
                symbol=symbol,
                type='market',
                side=side,
                amount=base_amount,
                params={'reduceOnly': False}
            )
        except ccxt.BaseError as e:
            if 'TE_ERR_INCONSISTENT_POS_MODE' in str(e):
                pos_side = 'Long' if side == 'buy' else 'Short'
                market_order = exchange.create_order(
                    symbol=symbol,
                    type='market',
                    side=side,
                    amount=base_amount,
                    params={
                        'reduceOnly': False,
                        'posSide': pos_side
                    }
                )
            else:
                raise

        thread_safe_print(f"✅ Market Order Placed: {market_order}")
        market_order_id = market_order.get('id')

        # 🟡 Fetch position and liquidation info
        positions = exchange.fetch_positions([symbol])
        pos_side_str = 'long' if side == 'buy' else 'short'
        position = next((p for p in positions if p['side'] == pos_side_str), None)

        if not position or not float(position.get('liquidationPrice') or 0):
            thread_safe_print("⚠️ No valid liquidation price found. Skipping limit order.")
            return market_order_id, None

        # Extract necessary fields
        liquidation_price = float(position.get('liquidationPrice'))
        entry_price = float(position.get('entryPrice') or 0)
        mark_price = float(position.get('markPrice') or 0)
        contracts = float(position.get('contracts') or 0)
        notional = float(position.get('notional') or 0)
        leverage = float(position.get('leverage') or 1)

        # 🧮 Determine precision
        price_precision = exchange.markets[symbol]['precision']['price']
        price_sig_digits = count_sig_digits(price_precision)
        amount_precision = exchange.markets[symbol]['precision']['amount']
        amount_sig_digits = count_sig_digits(amount_precision)

        thread_safe_print(f"📏 Price Precision: {price_precision}, Sig Digs: {price_sig_digits}")

        # 🔁 Calculate amount for limit order (2x notional size)
        double_notional = (notional / leverage) * multiplier
        order_amount = double_notional / mark_price
        order_amount = round_to_sig_figs(order_amount, amount_sig_digits)

        # 🎯 Calculate re-entry target price
        try:
            thread_safe_print("📐 Calculating re-entry target price...")
            target_price = calculateLiquidationTargPrice(entry_price, liquidation_price, fromPercnt, price_sig_digits)
            thread_safe_print(f"🎯 Target Price: {target_price}")
        except Exception as e:
            thread_safe_print(f"❌ Error calculating target price: {e}")
            return market_order_id, None

        # 🟢 Place limit order (with fallback to posSide)
        try:
            limit_order = exchange.create_order(
                symbol=symbol,
                type='limit',
                side=side,
                amount=contracts,
                price=target_price
            )
        except ccxt.BaseError as e:
            if 'TE_ERR_INCONSISTENT_POS_MODE' in str(e):
                limit_order = exchange.create_order(
                    symbol=symbol,
                    type='limit',
                    side=side,
                    amount=contracts,
                    price=target_price,
                    params={'posSide': pos_side_str.capitalize()}
                )
            else:
                raise

        thread_safe_print(f"📌 Limit Order Placed: {limit_order}")
        limit_order_id = limit_order.get('id')

        return market_order_id, limit_order_id

    except Exception as e:
        thread_safe_print(f"❌ Unexpected error for {symbol}: {e}")
        return None, None


stop_event = threading.Event()  # Global stop signal

# MAIN LOOP
def main_job(exchange, user_cred_id, token, verify):
    try:
        # drop_table("opn_trade")
        MAX_NO_SELL_TRADE = issueNumberOfTrade(account_balance)
        MAX_NO_BUY_TRADE = 2
        if stop_event.is_set():
            return

        signal = fetch_single_trade_signal()
        if not signal:
            thread_safe_print("ℹ️ No active signals available.")
            return
        
        trade_signal_id = signal['id']
        symbol = signal['symbol_pair']
        side = signal['trade_type']
        trail_thresh = 0.10 # 10% default
        profit_target_distance = 0.06 # 60% default
        
        if has_open_trade(user_cred_id, symbol):
            print(f"Trade {symbol} already taken for {user_cred_id}")
            return
        
        if has_open_position(exchange, symbol):
          print(f"⚠️ Skipping {symbol} — already has an open position on exchange")
          #reEnter Details here, for manual trades taken
          return
        
        
        position_count = get_side_count(user_cred_id, 0, side)
        print("Position Count: ", position_count)
        if side == 0 and position_count >= MAX_NO_BUY_TRADE:
            return
        elif side == 1 and position_count >= MAX_NO_SELL_TRADE:
            print(f"Max number of sell trade reached ({position_count})!")
            return
        
        # Implement your trade logic here for this signal
        thread_safe_print(f"✅ {verify if hasattr(exchange, 'id') else 'Exchange'} → Processing signal: {signal}")
        side_n_str = "buy" if side == 0 else "sell" if side == 1 else None
        
        usdt_balance = exchange.fetch_balance({'type': 'swap'})['USDT']['total']
        usdt_amount = calculateIntialAmount(usdt_balance, leverage)
        
        market_order_id, limit_order_id = place_entry_and_liquidation_limit_order(exchange, symbol, side_n_str, usdt_amount, leverage)

        base_amount = get_base_amount(exchange, symbol, usdt_amount)
        
        if market_order_id:
            take_trade_data= {
                "user_cred_id": user_cred_id,
                "trade_signal": trade_signal_id,
                "order_id": market_order_id,
                "symbol": symbol,
                "trade_type": side,
                "amount": base_amount,
                "leverage": leverage,
                "trail_threshold": trail_thresh,
                "profit_target_distance": profit_target_distance,
                "trade_done": 0,
                "status": 1
            }
            
            if insert_trade_signal(take_trade_data):
                # SEND BACKUP DATA TO MEDICTREAT.COM
                print("Token: ", token)
                backup_trade_data= {
                    "token": token,
                    "user_id": user_cred_id,
                    "order_id": market_order_id,
                    "symbol": symbol,
                    "trade_type": side,
                    "amount": base_amount,
                    "leverage": leverage,
                    "trail_threshold": trail_thresh,
                    "profit_target_distance": profit_target_distance,
                    "trade_done": 0,
                    "status": 1
                }
                save_trade_history(backup_trade_data)
           

        # Your trading execution logic (currently commented out)
        # 
        
    except Exception as e:
        thread_safe_print(f"Main job error: {e}")
        traceback.print_exc()

def run_exchanges_in_batch(batch):
    index = 0
    while not stop_event.is_set():
        try:
            exchange, user_cred_id, token, verify = batch[index % len(batch)]
            main_job(exchange, user_cred_id, token, verify)  # Takes one random prioritized signal
            index += 1
            # time.sleep(0.4)  # Delay between each exchange in this thread's batch
        except Exception:
            thread_safe_print("⚠️ Error in batch round-robin. Retrying...")
            traceback.print_exc()
            time.sleep(5)


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
            
            # token = generate_mixed_token(exchange_name, row['secret'], length=30)
            token = "constra2025X9bL7kDq8mNpTz3VwAeU61"
            exchange_list.append((exchange, row['cred_id'], token, verify))
        except Exception as e:
            print(row)
            thread_safe_print(f"❌ Failed to create exchange for API key {row['api_key'][:6]}...: {e}")

    if not exchange_list:
        thread_safe_print("⚠️ No valid exchanges could be created. Exiting...")
        return

    total = len(exchange_list)
    batch_size = max(1, int(math.sqrt(total)))  # √N batching for load balancing
    thread_safe_print(f"Total exchanges: {total}, Batch size: {batch_size}")

    batches = [exchange_list[i:i + batch_size] for i in range(0, total, batch_size)]

    threads = []
    for batch in batches:
        t = threading.Thread(target=run_exchanges_in_batch, args=(batch,), daemon=True)
        t.start()
        threads.append(t)

    try:
        while not stop_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        thread_safe_print("\n🛑 Ctrl+C detected, stopping all threads...")
        stop_event.set()

if __name__ == "__main__":
    run_all()
