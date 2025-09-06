import datetime
import os
import time
import pytz
import requests
import pandas as pd
from dotenv import load_dotenv
from config.logger import logger
from kiteconnect import KiteConnect
from apscheduler.schedulers.background import BackgroundScheduler

load_dotenv()


class Zerodha:
    def __init__(self, api_key: str, access_token: str):
        self.api_key = api_key
        self.access_token = access_token

        self.kite = KiteConnect(api_key=self.api_key)
        self.kite.set_access_token(access_token=access_token)
        self.url_index = "https://www.nseindia.com/api/option-chain-indices"
        self.url_equity = "https://www.nseindia.com/api/quote-equity"
        self.headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://www.nseindia.com/"
        }

        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def fetch_ltp(self, symbol="NIFTY", step=50):
        try:
            data = self.session.get(self.url_index, params={"symbol": symbol}).json()
            if not data:
                logger.error("No data found")
                return None
            ltp = data['records']['underlyingValue']
            if not ltp:
                logger.error("Unable to fetch LTP")
                return None
            return (round(ltp / step) * step) + step
        except Exception as e:
            logger.error(f"Unable to fetch LTP: {e}")
            return None

    def order_placement(self, trading_symbol, transaction_type, quantity=1, contract_type=None, step=None, expiry=None):
        try:
            if contract_type and step and expiry:
                exchange = self.kite.EXCHANGE_NFO
            else:
                exchange = self.kite.EXCHANGE_NSE
            entity = self.entity(trading_symbol=trading_symbol, step=step, contract_type=contract_type, expiry=expiry)
            logger.info(entity)
            base_order = self.kite.place_order(variety=self.kite.VARIETY_REGULAR,
                                               exchange=exchange,
                                               tradingsymbol=entity,
                                               transaction_type=transaction_type,
                                               quantity=quantity,
                                               order_type=self.kite.ORDER_TYPE_MARKET,
                                               product=self.kite.PRODUCT_MIS
                                               )

            time.sleep(1)
            order_history = self.kite.order_history(base_order)
            if not order_history:
                logger.error("Unable to fetch OrderBook")
                return

            logger.info(order_history[-1]["transaction_type"])
            entry_price = order_history[-1]['average_price']
            logger.info(entry_price)

            logger.info(f"Entry_price = {entry_price}")

            if order_history[-1]['transaction_type'] == self.kite.TRANSACTION_TYPE_SELL:
                target_price = entry_price * 0.15
                target_price = entry_price - target_price
                logger.info(f"Target price: {target_price}")
                sl_price = entry_price * 0.15
                sl_price = entry_price + sl_price

                logger.info(f"SL price: {sl_price}")
                sl_order = self.kite.place_order(variety=self.kite.VARIETY_REGULAR,
                                                 exchange=exchange,
                                                 tradingsymbol=entity,
                                                 transaction_type=self.kite.TRANSACTION_TYPE_BUY,
                                                 quantity=quantity,
                                                 order_type=self.kite.ORDER_TYPE_SL,
                                                 trigger_price=sl_price,
                                                 price=sl_price,
                                                 product=self.kite.PRODUCT_MIS)

                target_order = self.kite.place_order(variety=self.kite.VARIETY_REGULAR,
                                                     exchange=exchange,
                                                     tradingsymbol=entity,
                                                     transaction_type=self.kite.TRANSACTION_TYPE_BUY,
                                                     quantity=quantity,
                                                     order_type=self.kite.ORDER_TYPE_LIMIT,
                                                     price=target_price,
                                                     product=self.kite.PRODUCT_MIS
                                                     )
            else:
                sl_price = entry_price * 0.15
                sl_price = entry_price - sl_price
                logger.info(f"SL price: {sl_price}")
                target_price = entry_price * 0.15
                target_price = entry_price + target_price

                logger.info(f"Target price: {sl_price}")
                sl_order = self.kite.place_order(variety=self.kite.VARIETY_REGULAR,
                                                 exchange=exchange,
                                                 tradingsymbol=entity,
                                                 transaction_type=self.kite.TRANSACTION_TYPE_SELL,
                                                 quantity=quantity,
                                                 order_type=self.kite.ORDER_TYPE_SL,
                                                 trigger_price=sl_price,
                                                 price=sl_price,
                                                 product=self.kite.PRODUCT_MIS)

                target_order = self.kite.place_order(variety=self.kite.VARIETY_REGULAR,
                                                     exchange=exchange,
                                                     tradingsymbol=entity,
                                                     transaction_type=self.kite.TRANSACTION_TYPE_SELL,
                                                     quantity=quantity,
                                                     order_type=self.kite.ORDER_TYPE_LIMIT,
                                                     price=target_price,
                                                     product=self.kite.PRODUCT_MIS
                                                     )

            logger.info(f"buy_order_id: {base_order}, sl_order: {sl_order}, target_order: {target_order}")
            return base_order, sl_order, target_order
        except Exception as e:
            logger.error(f"Unable to place order: {e}")
            return

    def exit_positions(self):
        open_orders = self.order_book() or []
        if not isinstance(open_orders, list):
            logger.error("Order book response invalid")
            return
        for orders in open_orders:
            try:
                if orders['status'] == "OPEN" or orders['status'] == "TRIGGER PENDING":
                    self.kite.cancel_order(variety=orders["variety"],
                                           order_id=orders["order_id"])
                    logger.info(f"Cancelled order: {orders['tradingsymbol']} ({orders['order_id']})")
            except Exception as e:
                logger.error(f"Unable to cancel order: {e}")

        positions = self.position_book()
        if not positions:
            logger.warning("No position found")
            return

        for open_position in positions['net']:
            if open_position['quantity'] != 0:
                if open_position['quantity'] > 0:
                    transaction_type = self.kite.TRANSACTION_TYPE_SELL
                else:
                    transaction_type = self.kite.TRANSACTION_TYPE_BUY

                self.kite.place_order(variety=self.kite.VARIETY_REGULAR,
                                      exchange=open_position['exchange'],
                                      tradingsymbol=open_position['tradingsymbol'],
                                      transaction_type=transaction_type,
                                      quantity=abs(open_position['quantity']),
                                      order_type=self.kite.ORDER_TYPE_MARKET,
                                      product=self.kite.PRODUCT_MIS)

                logger.info(f"Exited position: {open_position['tradingsymbol']} ({open_position['quantity']})")

    def modify_order(self, order_id, modify_price, trigger_price=None, quantity=None):
        try:
            modify = self.kite.modify_order(variety=self.kite.VARIETY_REGULAR,
                                            order_id=order_id,
                                            price=modify_price,
                                            trigger_price=trigger_price,
                                            quantity=quantity)
            if not modify:
                logger.warning("No orders to Modify")
                return None
            logger.info(f"modify: {modify}")
            return modify
        except Exception as e:
            logger.error(f"Unable to modify order: {e}")
            return

    def cancel_order(self, order_id):
        try:
            cancel_order = self.kite.cancel_order(variety=self.kite.VARIETY_REGULAR,
                                                  order_id=order_id)
            if not cancel_order:
                logger.warning("No orders to cancel")
                return None
            logger.info(f"Cancelled: {cancel_order}")
            return cancel_order
        except Exception as e:
            logger.error(f"Unable to cancel order: {e}")
            return

    def order_book(self):
        try:
            orders = self.kite.orders()
            if not orders:
                logger.warning("No orders found")
                return None
            logger.info(f"Order book: {orders}")
            return orders
        except Exception as e:
            logger.error(f"Unable to fetch order book: {e}")
            return pd.DataFrame()

    def position_book(self):
        try:
            position = self.kite.positions()
            if not position:
                logger.warning("No position found")
                return None
            logger.info(f"Position book: {position}")
            return position
        except Exception as e:
            logger.error(f"Unable to fetch position book: {e}")
            return pd.DataFrame()

    def trade_book(self):
        try:
            trade_book = self.kite.trades()
            if not trade_book:
                logger.warning("No trades found")
                return None
            logger.info(f"Trade Book: {trade_book}")
            return trade_book
        except Exception as e:
            logger.error(f"Unable to fetch trade_book {e}")
            return pd.DataFrame()

    def get_atm_strike(self):
        try:
            data = self.kite.ltp(256265)
            logger.info(data)
        except Exception as e:
            logger.error(f"Unable get ATM strike: {e}")

    def fetch_nfo_contracts(self):
        try:
            data = self.kite.instruments("NFO")
            df = pd.DataFrame(data)
            return df
        except Exception as e:
            logger.error(f"Unable to fetch FNO contracts: {e}")
            return pd.DataFrame()



    def fetch_equity(self):
        try:
            data = self.kite.instruments("NSE")
            df = pd.DataFrame(data)
            return df
        except Exception as e:
            logger.error(f"Unable to fetch Equities:{e}")
            return pd.DataFrame()

    def entity(self, trading_symbol, step=None, contract_type=None, expiry=None):
        if expiry and contract_type:
            df = self.fetch_nfo_contracts()
            atm = self.fetch_ltp(trading_symbol, step)
            logger.info(atm)
            if isinstance(expiry, str):
                expiry = datetime.datetime.strptime(expiry, "%Y-%m-%d").date()

            filtered_df = df[
                (df['name'] == trading_symbol) &
                (df['expiry'] == expiry) &
                (df['strike'] == atm) &
                (df['instrument_type'] == contract_type)
                ]
            entity = filtered_df["tradingsymbol"].values[0]
            if not entity:
                logger.warning("No entity of that name found")
                return None
        else:
            df = self.fetch_equity()
            entity = df[df['tradingsymbol'] == trading_symbol]
            entity = entity['tradingsymbol'].values[0]
            if not entity:
                logger.warning("No entity of that name found")

        return entity


if __name__ == '__main__':
    zerodha = Zerodha(os.getenv("ZERODHA_API_KEY"), os.getenv("ZERODHA_ACCESS_TOKEN"))
    # exchange = "NSE"
    # trading_symbol = "NIFTY 50"
    # zerodha.fetch_symbols(exchange)
    # print(zerodha.get_instrument(exchange, trading_symbol))
    # exchange = "NFO"
    # zerodha.get_fo_contracts(exchange)
    # zerodha.get_atm_strike()
    #
    #
    # # logger.info(zerodha.fetch_ltp("BANKNIFTY"))
    # zerodha.order_placement("GOLDBEES", "BUY")

    # zerodha.order_book()
    # zerodha.position_book()
    # zerodha.trade_book()
    # # zerodha.modify_order(order_id=250905000330225, modify_price=74, trigger_price=76)
    # zerodha.cancel_order(order_id=250905000330230)

    # print(zerodha.fetch_nfo_contracts())
    # print(zerodha.entity("RELIANCE"))
    # print(zerodha.fetch_equity())
    # contract_type=None, step=None, expiry=None
    trading_symbol = input("Enter Trading Symbol: ").upper().strip()
    transaction_type = input("Enter Transaction Type: ").upper().strip()
    quantity = input("Enter quantity: ").upper().strip()
    contract_type = input("Enter contract type: ").upper().strip()
    user_input = input("Enter the number of steps: ")
    step = int(user_input) if user_input.strip() else None
    expiry = input("Enter date in this format '%Y-%m-%d': ")
    scheduler = BackgroundScheduler(timezone=pytz.timezone("Asia/Kolkata"))
    scheduler.start()
    scheduler.add_job(zerodha.order_placement, 'cron', day_of_week='mon-sat', hour=16, minute=2,
                      args=[trading_symbol, transaction_type, quantity, contract_type, step, expiry])

    scheduler.add_job(zerodha.exit_positions, 'cron', day_of_week='mon-sat', hour=16, minute=3)
    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
