import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import asyncio
import time
from utils.SqlManager import PolymarketTradeManager,AsyncPolymarketTradeManager
from utils.create_client import get_clob_client

from utils.inquire_target_wallet import append_trades
from utils.setup_approver import setup_approvals,approve_usdc
from utils.TradeThread import PollingThread,TradingThread,polling_task,trading_task
from py_clob_client.clob_types import BalanceAllowanceParams,AssetType

from utils.config import PRIVATE_KEY,MAIN_SPENDER,USDC_ADDRESS,POLYGON_RPC


from web3 import Web3
from eth_account import Account
import time
from web3 import Web3
from eth_account import Account








import asyncio
# ... 其他导入 ...

async def main():
    # 1. 初始化 client (由于 get_clob_client 是 async)
    client = await get_clob_client()
    
    # 2. 初始化 SQL
    AsyncPolymarketTradeManager()

     # 3. 授权 
    w3 = Web3(Web3.HTTPProvider(POLYGON_RPC))
    approve_usdc(w3,PRIVATE_KEY,MAIN_SPENDER,USDC_ADDRESS)
    #setup_approvals() 
    
    # 4. 启动线程
    #polling = PollingThread(interval=10.0)
    #trading = TradingThread(client,  interval=0.2)
    #polling.run()
    #trading.run()
    await asyncio.gather(
        polling_task(10.0),
        trading_task(client, 0.2),#    处理tokenid 结束了的订单
        return_exceptions=True   
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("收到 Ctrl+C，程序退出")



