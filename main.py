

MARKET_ID = "BTC-USD"
WALLET_MNEMONIC = ( "test test test test test test "
                    "test test test test test test "
                    "test test test test test test "
                    "test test test test test test" )

WALLET_ADDRESS = "<your dydx address here>"


rest_indexer = "https://indexer.dydx.trade"
websocket_indexer = "indexer.dydx.trade/v4/ws"

node_urls = [
    "dydx-grpc.publicnode.com:443",
    "dydx-dao-grpc.polkachu.com:443",
    "dydx-ops-grpc.kingnodes.com:443",
    "dydx-grpc.lavenderfive.com:443",
    "dydx-mainnet-grpc.autostake.com:443",
    "grpc-dydx.ecostake.com:443",
    "dydx-grpc.publicnode.com:443",
]

global node;

for url in node_urls:
    try:
        network = make_mainnet(rest_indexer, websocket_indexer, url)
        node = await NodeClient.connect(network.node)
        await node.latest_block()
        print("Connected to RPC node " + url)
        break;
    except Exception as e:
        print("URL "+url+" failed to connect.")
        print(e)
        
indexer = IndexerClient(rest_indexer)

market = Market(
        (await indexer.markets.get_perpetual_markets(MARKET_ID))["markets"][MARKET_ID]
)
wallet = await Wallet.from_mnemonic(node, WALLET_MNEMONIC, WALLET_ADDRESS)


# Market and account functions

async def get_candles(resolution, fromISO):
    response = await indexer.markets.get_perpetual_market_candles(MARKET_ID, resolution, fromISO)
    return response['candles']

async def get_last_price():
    response = await indexer.markets.get_perpetual_market_trades(MARKET_ID,limit=1)
    trades = response['trades']
    trades = pd.json_normalize(trades)
    return int(trades["price"].iloc[0])

async def close_all_positions(now, latest_price):
    response = await indexer.account.get_subaccount_perpetual_positions(WALLET_ADDRESS,0,status="OPEN")
    response = pd.json_normalize(response["positions"])
    if ( response.shape[0] > 0 ):
        amt = float(response.iloc[0]["size"]);
        if (response.iloc[0]["side"] == "LONG"):
            await place_limit_order(2,latest_price,amt,int(now+60*8)); # exipre time should be slight below interval_3
        elif (response.iloc[0]["side"] == "SHORT"):
            await place_limit_order(1,latest_price,amt,int(now+60*8));

async def place_market_order(id, size, expire_unixtime):
    
    '''
    if ( side.upper() == "BUY" ):
        side = Order.Side.SIDE_BUY;
    elif ( side.upper() == "SELL" ):
        side = Order.Side.SIDE_SELL;
    else:
        return;
    '''
    
    global node;
    
    if ( id % 2 == 1 ):
        side = Order.Side.SIDE_BUY;
    else:
        side = Order.Side.SIDE_SELL;
        
    #order_id = market.order_id(
    #    WALLET_ADDRESS, 0, random.randint(0, MAX_CLIENT_ID), OrderFlags.LONG_TERM
    #)
    
    order_id = market.order_id(
        WALLET_ADDRESS, 0, id, OrderFlags.LONG_TERM
    )

    try:
        current_block = await node.latest_block_height()
    except:
        for url in node_urls:
            try:
                network = make_mainnet(rest_indexer, websocket_indexer, url)
                node = await NodeClient.connect(network.node)
                current_block = await node.latest_block_height()
                print("Re-connected to RPC node " + url)
                break;
            except:
                print("URL "+url+" failed to connect.")
        

    new_order = market.order(
        order_id=order_id,
        order_type=OrderType.MARKET,
        side=side,
        size=size,
        price=0,
        time_in_force=Order.TimeInForce.TIME_IN_FORCE_UNSPECIFIED,
        reduce_only=False,
        good_til_block_time=expire_unixtime
    )

    transaction = await node.place_order(
        wallet=wallet,
        order=new_order,
    )

    print("Placed market order");
    print(transaction)
    wallet.sequence += 1
    time.sleep(2)
    
    
    
async def place_limit_order(id, price, size, expire_unixtime):
    
    global node;
    
    #if ( side.upper() == "BUY" ):
    #    side = Order.Side.SIDE_BUY;
    #elif ( side.upper() == "SELL" ):
    #    side = Order.Side.SIDE_SELL;
    #else:
    #    return;
    
    if ( id % 2 == 1 ):
        side = Order.Side.SIDE_BUY;
    else:
        side = Order.Side.SIDE_SELL;
            
    order_id = market.order_id(
        WALLET_ADDRESS, 0, id, OrderFlags.LONG_TERM
    )
    
    try:
        current_block = await node.latest_block_height()
    except:
        for url in node_urls:
            try:
                network = make_mainnet(rest_indexer, websocket_indexer, url)
                node = await NodeClient.connect(network.node)
                current_block = await node.latest_block_height()
                print("Re-connected to RPC node " + url)
                break;
            except:
                print("URL "+url+" failed to connect.")

    new_order = market.order(
        order_id=order_id,
        order_type=OrderType.LIMIT,
        side=side,
        size=size,
        price=price,
        time_in_force=Order.TimeInForce.TIME_IN_FORCE_UNSPECIFIED,
        reduce_only=False,
        good_til_block_time=expire_unixtime
    )

    transaction = await node.place_order(
        wallet=wallet,
        order=new_order,
    )

    print("Placed limit order at " + str(price))
    print(transaction)
    wallet.sequence += 1
    time.sleep(2)

    
    
async def close_open_limits(now, latest_price):
    
    market_amt = 0;
    
    response = await indexer.account.get_subaccount_orders(WALLET_ADDRESS,0,limit=100)
    orders = pd.json_normalize(response)    
    orders = orders[orders['goodTilBlockTime'].notna()]
    
    print(orders)

    for index, row in orders.iterrows():
        
        diff = now - dp.parse(row["goodTilBlockTime"]).timestamp()
        
        if (int(row["clientId"]) < 100): # Take into account any unfilled "market" orders
            if ( (row['status'] == 'CANCELED') & (row['side'] == 'BUY') ):
                market_amt += float(row['size'])
            elif ( (row['status'] == 'CANCELED') & (row['side'] == 'SELL') ):
                market_amt -= float(row['size'])
        elif( (diff > 0) & (diff < 90) ): #expired within last 90 seconds  
            if ( (row['status'] == 'FILLED') & (row['side'] == 'BUY') ):
                print("BUY Order "+row['clientId']+" was filled in this cycle. Placing market order.")
                market_amt -= float(row['totalFilled']) # If bought previously, need to sell
            elif ( (row['status'] == 'FILLED') & (row['side'] == 'SELL') ):
                print("SELL Order "+row['clientId']+" was filled in this cycle. Placing market order.")
                market_amt += float(row['totalFilled'])


    if ( market_amt > 0 ):
        #await place_market_order(1,abs(market_amt),now+60);
        await place_limit_order(1,latest_price*1.10,abs(market_amt),int(now+interval_1-30))
    elif ( market_amt < 0 ):
        #await place_market_order(2,abs(market_amt),now+60);
        await place_limit_order(2,latest_price*0.9,abs(market_amt),int(now+interval_1-30))


last_price = 0
last_high = 0
last_low = 0

def forecast_high_ml():
    return last_price

def forecast_low_ml():
    return last_price

def forecast_high_simple():
    return last_price + ( last_high - last_low )*0.5
        
def forecast_low_simple():
    return last_price - ( last_high - last_low )*0.5

def calc_profit_vs_fees( high, low, amt ):
    slippage = 30; # Just an estimate. Comes to about 0.03%
    avg_profit = (high - low - slippage)*0.5*amt; # Assume hit one side and half way to other side
    avg_fees = (.0005+.00012)*amt*high # On average with this algorithm, we should expect 1 maker, 1 taker
    print('Potential profit is '+str(avg_profit))
    print('Avg fees would be '+str(avg_fees))
    if (avg_profit > avg_fees):
        return True;
    else:
        return False;

btc_total = 0.01; # Set max amount of btc to trade with

interval_1 = 60*5 #60*40 Select number of minutes for smallest trading window
interval_2 = interval_1 * 6
interval_3 = interval_2 * 6
interval_4 = interval_3 * 7  # Weekly ... only used for risk management
interval_5 = interval_4 * 9  # Monthly ... ""
interval_6 = interval_5 * 12 # Yearly ... ""
interval_7 = interval_6 * 15 # Entire btc lifespan

run_interval_1 = False;
run_interval_2 = False;
run_interval_3 = False;

next_profit_norm_1 = 0;
next_profit_norm_2 = 0;
next_profit_norm_3 = 0;

while(True):
    print("************** NEW LOOP ITERATION ****************")
    now = (datetime.utcnow() - datetime(1970, 1, 1)).total_seconds();
    last_price = await get_last_price()
    
    run_interval_1 = True;
    if ( now % interval_2 <= interval_1 ):
        run_interval_2 = True;
        if ( now % interval_3 <= interval_2 ):
            await close_all_positions(now,last_price); # Reverse full position to 0
            run_interval_3 = True;
        else:
            await close_open_limits(now,last_price); # Reverse only necessary recent positions
    else:
        await close_open_limits(now,last_price); # Reverse only necessary recent positions
        
    
    past_candles = pd.read_csv('dydx_1min_candles.csv', sep=',', header=0)
    past_candles.set_index("Timestamp",inplace=True)

    #start_time = past_candles.iloc[0]["Timestamp"]
    start_time = 1736626740
    now_rounded = now - (now%60);

    #num_mins = (now - start_time)/60

    i = now_rounded
    new_candles_df = pd.DataFrame()
    while( i >= start_time ):
        if( i not in past_candles.index ):
            i -= 60;
        else:
            #fromISO = past_candles.loc(i)["startedAt"];
            fromISO = datetime.utcfromtimestamp(i+60).isoformat()
            new_candles = await get_candles("1MIN",fromISO)
            new_candles_df = pd.json_normalize(new_candles)
            break;

    if ( new_candles_df.shape[0] == 0 ):
        now2 = (datetime.utcnow() - datetime(1970, 1, 1)).total_seconds();
        time.sleep(interval_1 - (now2-now));
        continue;
        
    timestamps = []
    for index, row in new_candles_df.iterrows():
        timestamp = dp.parse(row["startedAt"]).timestamp()
        timestamps.append(int(timestamp))

    new_candles_df["Timestamp"] = timestamps
    new_candles_df.set_index('Timestamp', inplace=True)

    new_candles_df = new_candles_df[['low','high','open']]

    candles_df = pd.DataFrame();
    candles_df = pd.concat([new_candles_df, past_candles])

    candles_df.to_csv('dydx_1min_candles.csv', index=True);

    print("Last price: " + str(last_price))
    
    if(run_interval_1):
        index_1 = candles_df[ candles_df.index >= now - interval_1 ]
        last_low = min( list(map(int, index_1["low"] )) )
        last_high = max( list(map(int, index_1["high"] )) )
        next_high_1 = forecast_high_simple()
        next_low_1 = forecast_low_simple()
        next_profit_norm_1 = next_high_1 - next_low_1
    
    if(run_interval_2):
        index_2 = candles_df[ candles_df.index >= now - interval_2 ]
        last_low = min( list(map(int, index_2["low"] )) )
        last_high = max( list(map(int, index_2["high"] )) )
        next_high_2 = forecast_high_simple()
        next_low_2 = forecast_low_simple()
        next_profit_norm_2 = (next_high_2 - next_low_2) / (float(interval_2) / float(interval_1))
    
    if(run_interval_3):
        index_3 = candles_df[ candles_df.index >= now - interval_3 ]
        last_low = min( list(map(int, index_3["low"] )) )
        last_high = max( list(map(int, index_3["high"] )) )
        next_high_3 = forecast_high_simple()
        next_low_3 = forecast_low_simple()
        next_profit_norm_3 = (next_high_3 - next_low_3) / (float(interval_3) / float(interval_1))
        
    sum_profit_norm = next_profit_norm_1 + next_profit_norm_2 + next_profit_norm_3
    
    # Used for risk management
    sum_profit_norm += (102712.0 - 91220) / (float(interval_4) / float(interval_1)) # Last FULL weekly candle on CMM
    sum_profit_norm += (108268.0 - 91317) / (float(interval_5) / float(interval_1)) # Last full monthly candle on CMM
    sum_profit_norm += (108268.0 - 49121) / (float(interval_6) / float(interval_1)) # Min/Max over last year
    sum_profit_norm += (108268.0 - 1) / (float(interval_7) / float(interval_1)) # Entire btc span
        
    if(run_interval_3):
        next_amt_3 = (next_profit_norm_3 / sum_profit_norm) * btc_total
        print("Next Int 3 High: " + str(next_high_3))
        print("Next Int 3 Low: " + str(next_low_3))
        print("Next Int 3 Amt: "+str(next_amt_3))
        if ( calc_profit_vs_fees(next_high_3, next_low_3, next_amt_3) ):
            await place_limit_order(302, next_high_3, next_amt_3, int(now+interval_3 - 150))
            await place_limit_order(301, next_low_3, next_amt_3, int(now+interval_3 - 150))
        else:
            print("Potential profit not worth the fees. Skipping Interval 3.")
            
    if(run_interval_2):
        next_amt_2 = (next_profit_norm_2 / sum_profit_norm) * btc_total
        print("Next Int 2 High: " + str(next_high_2))
        print("Next Int 2 Low: " + str(next_low_2))
        print("Next Int 2 Amt: "+str(next_amt_2))
        if ( calc_profit_vs_fees(next_high_2, next_low_2, next_amt_2) ):
            await place_limit_order(202, next_high_2, next_amt_2, int(now+interval_2 - 80))
            await place_limit_order(201, next_low_2, next_amt_2, int(now+interval_2 - 80))
        else:
            print("Potential profit not worth the fees. Skipping Interval 2.")
        
    if(run_interval_1):
        next_amt_1 = (next_profit_norm_1 / sum_profit_norm) * btc_total
        print("Next Int 1 High: " + str(next_high_1))
        print("Next Int 1 Low: " + str(next_low_1))
        print("Next Int 1 Amt: "+str(next_amt_1))
        if ( calc_profit_vs_fees(next_high_1, next_low_1, next_amt_1) ):
            await place_limit_order(102, next_high_1, next_amt_1, int(now+interval_1 - 20))
            await place_limit_order(101, next_low_1, next_amt_1, int(now+interval_1 - 20))
        else:
            print("Potential profit not worth the fees. Skipping Interval 1.")
    
    run_interval_1 = False;
    run_interval_2 = False;
    run_interval_3 = False;

    
    now2 = (datetime.utcnow() - datetime(1970, 1, 1)).total_seconds();
    time.sleep(interval_1 - (now2-now)); # Sleep until next iteration





    


