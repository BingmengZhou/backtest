import datetime
import backtrader as bt  
import oss2 
from oss2.credentials import EnvironmentVariableCredentialsProvider 
import pandas as pd 
import argparse
from backtrader.analyzers import SharpeRatio, DrawDown
import json
from alibabacloud_credentials.client import Client
from alibabacloud_credentials.models import Config
from oss2 import CredentialsProvider
from oss2.credentials import Credentials
GLOBAL_OSS_END_POINT='oss-cn-shanghai-internal.aliyuncs.com'
GLOBAL_OSS_BUCKET='whitepaper-quant'

class CredentialProviderWarpper(CredentialsProvider):
    def __init__(self, client):
        self.client = client

    def get_credentials(self):
        access_key_id = self.client.get_access_key_id()
        access_key_secret = self.client.get_access_key_secret()
        security_token = self.client.get_security_token()
        return Credentials(access_key_id, access_key_secret, security_token)

def InitailizeData(stock_name):
    """
    初始化数据函数
    从阿里云OSS服务中读取指定股票名称的CSV数据，并将其转换为对应的pandas DataFrame格式，设置好时间索引，最后配置为backtrader数据feed。

    参数:
    stock_name - 字符串类型，指定的股票名称，用于构建CSV文件名。

    返回值:
    data - backtrader数据feed对象，可供策略回测使用。
    """
    # 配置OSS认证信息
    ##auth = oss2.ProviderAuth(EnvironmentVariableCredentialsProvider())
    ##auth =  oss2.Auth()
    config = Config(
    type='ecs_ram_role',      # 访问凭证类型。固定为ecs_ram_role。
    role_name='ecs-read-oss'    # 为ECS授予的RAM角色的名称。可选参数。如果不设置，将自动检索。强烈建议设置，以减少请求。
)   

    cred = Client(config)

    credentials_provider = CredentialProviderWarpper(cred)
    auth = oss2.ProviderAuth(credentials_provider)

   # 创建OSS Bucket对象，用于后续读取文件.请将此处的配置改为您个人存放csv行情数据的
    bucket = oss2.Bucket(auth, GLOBAL_OSS_END_POINT, GLOBAL_OSS_BUCKET)
    # 从OSS读取指定股票名称的CSV文件
    obj = bucket.get_object(stock_name+".csv")
    
    # 读取CSV数据到DataFrame，指定列名并跳过首行
    df_data = pd.read_csv(obj, 
                        names=['datetime', 'open', 'high', 'low', 'close', 'volume'],
                        skiprows=1) 
    # 将'datetime'列转换为datetime类型，并设置为DataFrame的索引
    df_data['datetime'] = pd.to_datetime(df_data['datetime'], unit='ms')
    df_data.set_index('datetime', inplace=True)
    print(df_data)
    # 配置数据feed，用于backtrader回测
    data = bt.feeds.PandasData(
        dataname=df_data,
        fromdate=datetime.datetime(2021,1,1),
        todate = datetime.datetime(2022,7,1))
    return bucket,data

class Strat2_BGTMA_SLSMA(bt.Strategy):
    """
    一个基于测试的策略类。
    
    参数:
    - maperiod: 用于简单移动平均线指标的周期数。
    - printlog: 是否打印交易策略的日志信息。
    """
    
    params = (
        ('maperiod',15), # 策略需要的任意变量设置。
        ('printlog',False), # 是否停止打印交易策略日志。
    )
    
    def __init__(self):
        """
        初始化函数，设置初始值和添加简单移动平均线指标。
        """
        self.dataclose= self.datas[0].close    # 保存数据序列中“close”的引用
        self.order = None # 用于跟踪挂单的属性。初始化时没有订单。
        self.buyprice = None
        self.buycomm = None
        
        # 添加简单移动平均线指标用于交易策略
        self.sma = bt.indicators.SimpleMovingAverage( 
            self.datas[0], period=self.params.maperiod)
    
    def log(self, txt, dt=None, doprint=False):
        """
        记录日志信息。
        
        参数:
        - txt: 要记录的文本信息。
        - dt: 日期时间对象，如果未提供，则使用数据序列的起始日期时间。
        - doprint: 是否强制打印日志，默认根据策略的printlog参数决定。
        """
        if self.params.printlog or doprint: 
            dt = dt or self.datas[0].datetime.date(0)
            print('{0},{1}'.format(dt.isoformat(),txt))
    
    def notify_order(self, order):
        """
        订单通知处理函数，处理订单的执行、取消、拒绝或因保证金不足等情况。
        """
        # 如果订单提交或接受，不做任何操作
        if order.status in [order.Submitted, order.Accepted]:
            return
        # 如果订单完成执行，记录买入/卖出执行价格等信息
        if order.status in [order.Completed]: 
            if order.isbuy():
                self.log('BUY EXECUTED, Price: {0:8.2f}, Size: {1:8.2f} Cost: {2:8.2f}, Comm: {3:8.2f}'.format(
                    order.executed.price,
                    order.executed.size,
                    order.executed.value,
                    order.executed.comm))
                
                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:
                self.log('SELL EXECUTED, {0:8.2f}, Size: {1:8.2f} Cost: {2:8.2f}, Comm{3:8.2f}'.format(
                    order.executed.price, 
                    order.executed.size, 
                    order.executed.value,
                    order.executed.comm))
            
            self.bar_executed = len(self) # 记录交易执行的bar位置
        # 如果订单被取消、因保证金不足或被拒绝，记录信息
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')
            
        self.order = None
    
    def notify_trade(self,trade):
        """
        成交通知处理函数，计算每次操作的毛利润和净利润。
        
        参数:
        - trade: 成交对象。
        """
        if not trade.isclosed:
            return
        
        self.log('OPERATION PROFIT, GROSS {0:8.2f}, NET {1:8.2f}'.format(
            trade.pnl, trade.pnlcomm))
    
    def next(self):
        """
        下一个周期处理函数，根据当前价格和移动平均线位置决定买卖操作。
        """
        # 记录数据序列关闭价格的日志
        self.log('Close, {0:8.2f}'.format(self.dataclose[0]))

        if self.order: # 检查是否有挂单，如果有则退出
            return
                
        # 如果不在市场中
        if not self.position: 
            if self.dataclose[0] > self.sma[0]:
                self.log('BUY CREATE {0:8.2f}'.format(self.dataclose[0]))
                self.order = self.buy()           
        else: # 如果在市场中
            if self.dataclose[0] < self.sma[0]:
                self.log('SELL CREATE, {0:8.2f}'.format(self.dataclose[0]))
                self.order = self.sell()
                
    def stop(self):
        """
        策略结束时调用的函数，打印最终资产价值和移动平均线周期数。
        """
        self.log('MA Period at {0:4.2f}, Final asset Value: {1:8.2f}'.format(
            self.params.maperiod, 
            self.broker.getvalue()),
                 doprint=True)
        ##global_results[self.params.maperiod]=self.broker.getvalue()
        ##print(global_results)

if __name__ == '__main__':   
    parser = argparse.ArgumentParser(description="Example script with variable arguments")
    parser.add_argument('positional_args', nargs='*', type=int, help='Variable number of positional arguments')
    parser.add_argument('--start', type=int, help='Start parameter', default=5)
    parser.add_argument('--end', type=int, help='End parameter', default=10)
    parser.add_argument('--stock', type=str, help='stock or asset name', default='aave.csv')
    args = parser.parse_args()
    bucket, data = InitailizeData(args.stock)
    cerebro = bt.Cerebro(optreturn=False)

    # 添加分析器
    cerebro.addanalyzer(SharpeRatio, _name='sharpe')
    cerebro.addanalyzer(DrawDown, _name='maxdrawdown')

    cerebro.adddata(data) 
    cerebro.optstrategy(
        Strat2_BGTMA_SLSMA,
        maperiod=range(args.start, args.end),
        printlog=False)
    cerebro.addsizer(bt.sizers.FixedSize, stake=10)
    cerebro.broker.setcash(1000.0)
    cerebro.broker.setcommission(commission=0.0)
    strategies = cerebro.run(maxcpus=2)
    results = {}
    for strat in strategies:
        for s in strat:
            ma_period = s.params.maperiod
            final_value = s.broker.getvalue()
            results[ma_period] = final_value
    ##回测结果写入OSS。results/标的名字
    bucket.put_object('results/'+args.stock+'-results.json',json.dumps(results, indent=4))

