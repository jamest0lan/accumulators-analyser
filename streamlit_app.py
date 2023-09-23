import streamlit as st
import pandas as pd
import math
from collections import namedtuple
import altair as alt
import requests
import time
from datetime import datetime, timedelta

"""
# Welcome to Accumulators Analyser!

Enter a token below to find the biggest accumulators over the past 7 days.

If you have any questions or notice any errors text me on [Twitter](https://twitter.com/JamesT0lan).

Data Information: 

This tool is analysing data over the past 7 days. CEX label identifies transactions from Binance, Crypto.com, Kucoin, Huobi, OKX, Kraken, Coinbase. Dashboard is in an early stage which may lead to some inaccuracies, some wallets might be misidentified by filters.

Powered by the [Syve.ai](https://www.syve.ai/) API. 
"""

# Defining global variables to call in another cell
global accumulators, fresh_wallets

def get_sql_api_response(api_url, headers, query):
    """Make a POST request to the API and return the JSON response."""
    try:
        response = requests.post(api_url, headers=headers, json=query)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"API request failed: {e}")
        return None

def get_filter_api_response(filter_api, address):
    """Make a get request to the filter API and return the json response"""
    try:
        response = requests.get(filter_api + address)
        response.raise_for_status()
        return response.json()   
    except requests.exceptions.RequestException as d:
        print(f"API request failed: {d}")
        return None

def create_dataframe(data):
    """Create a pandas DataFrame from the API response data."""
    if data:
        return pd.DataFrame(data)
    else:
        return pd.DataFrame()

def create_accumulators_table(df_in, df_out):
    """Calculate token changes and sort the DataFrame."""
    
    try:
        accumulators = pd.merge(df_in, df_out, on='address', how='left')
        accumulators.fillna(0, inplace=True)
        accumulators["Accumulated"] = accumulators["tokens_in"] - accumulators["tokens_out"]
        accumulators.sort_values(by='Accumulated', ascending=False, inplace=True)
        accumulators.rename(columns={'address':'from_address'}, inplace=True)
        accumulators = accumulators[accumulators["Accumulated"] > 0].reset_index(drop=True)
        return accumulators
    except:
        print('Error creating accumulators table. Data might not be available on this token.')
        quit()

def add_first_tx_time(filter_api, address, dataframe):
    """Add first transaction time of an address to a dataframe"""
    tx_data = get_filter_api_response(filter_api, address)
    tx_df = create_dataframe(tx_data)
        
    if tx_df.empty:
        return dataframe
    else:
        tx_df = tx_df[['timestamp', 'from_address']]
        tx_df = tx_df.groupby('from_address')['timestamp'].agg(min_time='min').reset_index()
        return pd.concat([dataframe, tx_df], ignore_index=True)

def create_fresh_wallets_df(accumulators_df, filter_api):
    
    global fresh_wallets
    
    # Get the first transaction time of each 7d accumulator and add it to a min_times df
    min_times = pd.DataFrame(columns=['from_address', 'min_time'])
    count = 1
    
    for address in accumulators_df['from_address']:
        if count % 5 == 0:
            time.sleep(1)
        
        min_times = add_first_tx_time(filter_api, address, min_times)

        count += 1
        
    min_times['min_date'] = pd.to_datetime(min_times['min_time'], unit='s').dt.strftime('%Y-%m-%d %H:%M:%S')
    min_times.drop('min_time', axis=1, inplace=True)
    
    # Get the current time
    now = datetime.now()

    # Define the time range (last 7 days)
    start_date = now - timedelta(days=7)

    # Filter the fresh wallets dataframe
    min_times['min_date'] = pd.to_datetime(min_times['min_date'])
    fresh_wallets = min_times[min_times['min_date'] > start_date]
    fresh_wallets = pd.merge(fresh_wallets, accumulators_df, on='from_address', how='inner')
    fresh_wallets.drop(['tokens_in','tokens_out'], axis=1, inplace=True)
    fresh_wallets.reset_index(drop=True, inplace=True)
    
    return fresh_wallets
    
def label_fresh_wallets():
    # Create a new column with default value 'N/A'
    accumulators["fresh_wallet_labels"] = '-'
    
    for address in accumulators["from_address"]:
        if address in fresh_wallets["from_address"].values:
            accumulators.loc[accumulators["from_address"] == address, "fresh_wallet_labels"] = 'Fresh Wallet'

def create_cex_labels(token_address):
    
    # Define CEX addresses
    
    binance_addresses = [
    '0x01c952174c24e1210d26961d456a77a39e1f0bb0',
    '0x161ba15a5f335c9f06bb5bbb0a9ce14076fbb645',
    '0x1fbe2acee135d991592f167ac371f3dd893a508b',
    '0x21a31ee1afc51d94c2efccaa2092ad1028285549',
    '0x28c6c06298d514db089934071355e5743bf21d60',
    '0x29bdfbf7d27462a2d115748ace2bd71a2646946c',
    '0x3c783c21a0383057d128bae431894a5c19f9cf06',
    '0x47ac0Fb4F2D84898e4D9E7b4DaB3C24507a6D503',
    '0x4976a4a02f38326660d17bf34b431dc6e2eb2327',
    '0x515b72ed8a97f42c568d6a143232775018f133c8',
    '0x56eddb7aa87536c09ccc2793473599fd21a8b17f',
    '0x5a52e96bacdabb82fd05763e25335261b270efcb',
    '0x73f5ebe90f27b46ea12e5795d16c4b408b19cc6f',
    '0x8894e0a0c962cb723c1976a4421c95949be2d4e3',
    '0x9696f59e4d72e237be84ffd425dcad154bf96976',
    '0xBE0eB53F46cd790Cd13851d5EFf43D12404d33E8',
    '0xa180fe01b906a1be37be6c534a3300785b20d947',
    '0xa344c7aDA83113B3B56941F6e85bf2Eb425949f3',
    '0xbd612a3f30dca67bf60a39fd0d35e39b7ab80774',
    '0xdccf3b77da55107280bd850ea519df3705d1a75a',
    '0xdfd5293d8e347dfe59e90efd55b2956a1343963d',
    '0xe2fc31f816a9b94326492132018c3aecc4a93ae1',
    '0xeb2d2f1b8c558a40207669291fda468e50c8a0bb',
    '0xf977814e90da44bfa03b6295a0616a897441acec',
    ]

    crypto_com_address = [
    '0x72A53cDBBcc1b9efa39c834A540550e23463AAcB',
    '0x7758e507850da48cd47df1fb5f875c23e3340c50',
    '0xcffad3200574698b78f32232aa9d63eabd290703',
    '0x6262998Ced04146fA42253a5C0AF90CA02dfd2A3',
    ]

    kucoin_addresses = [
    '0x03e6fa590cadcf15a38e86158e9b3d06ff3399ba',
    '0x14ea40648fc8c1781d19363f5b9cc9a877ac2469',
    '0x1692e170361cefd1eb7240ec13d048fd9af6d667',
    '0x2a8c8b09bd77c13980495a959b26c1305166a57f',
    '0x738cf6903e6c4e699d1c2dd9ab8b67fcdb3121ea',
    '0x88bd4d3e2997371bceefe8d9386c6b5b4de60346',
    '0xa3f45e619ce3aae2fa5f8244439a66b203b78bcc',
    '0xb8e6d31e7b212b2b7250ee9c26c56cebbfbe6b23',
    '0xcad621da75a66c7a8f4ff86d30a2bf981bfc8fdd',
    '0xd6216fc19db775df9774a6e33526131da7d19a2c',
    '0xd89350284c7732163765b23338f2ff27449e0bf5',
    '0xebb8ea128bbdff9a1780a4902a9380022371d466',
    '0xec30d02f10353f8efc9601371f56e808751f396f',
    '0xf16e9b0d03470827a95cdfd0cb8a8a3b46969b91',
    '0xf3f094484ec6901ffc9681bcb808b96bafd0b8a8',
    ]
    
    okx_addresses = [
    '0x06959153b974d0d5fdfd87d561db6d8d4fa0bb0b',
    '0x06d3a30cbb00660b85a30988d197b1c282c6dcb6',
    '0x276cdba3a39abf9cedba0f1948312c0681e6d5fd',
    '0x313eb1c5e1970eb5ceef6aebad66b07c7338d369',
    '0x3d55ccb2a943d88d39dd2e62daf767c69fd0179f',
    '0x42436286a9c8d63aafc2eebbca193064d68068f2',
    '0x461249076b88189f8ac9418de28b365859e46bfd',
    '0x4b4e14a3773ee558b6597070797fd51eb48606e5',
    '0x4d19c0a5357bc48be0017095d3c871d9afc3f21d',
    '0x4e7b110335511f662fdbb01bf958a7844118c0d4',
    '0x5041ed759dd4afc3a72b8192c143f72f4724081a',
    '0x539c92186f7c6cc4cbf443f26ef84c595babbca1',
    '0x5c52cc7c96bde8594e5b77d5b76d042cb5fae5f2',
    '0x62383739d68dd0f844103db8dfb05a7eded5bbe6',
    '0x65a0947ba5175359bb457d3b34491edf4cbf7997',
    '0x68841a1806ff291314946eebd0cda8b348e73d6d',
    '0x69a722f0b5da3af02b4a205d6f0c285f4ed8f396',
    '0x69a722f0b5da3af02b4a205d6f0c285f4ed8f396',
    '0x6fb624b48d9299674022a23d92515e76ba880113',
    '0x7e4aa755550152a522d9578621ea22edab204308',
    '0x7eb6c83ab7d8d9b8618c0ed973cbef71d1921ef2',
    '0x868dab0b8e21ec0a48b726a1ccf25826c78c6d7f',
    '0x96fdc631f02207b72e5804428dee274cf2ac0bcd',
    '0x9723b6d608d4841eb4ab131687a5d4764eb30138',
    '0x98ec059dc3adfbdd63429454aeb0c990fba4a128',
    '0xa7efae728d2936e78bda97dc267687568dd593f3',
    '0xbda23b750dd04f792ad365b5f2a6f1d8593796f2',
    '0xbf94f0ac752c739f623c463b5210a7fb2cbb420b',
    '0xbfbbfaccd1126a11b8f84c60b09859f80f3bd10f',
    '0xc3ae71fe59f5133ba180cbbd76536a70dec23d40',
    '0xc5451b523d5fffe1351337a221688a62806ad91a',
    '0xc708a1c712ba26dc618f972ad7a187f76c8596fd',
    '0xcba38020cd7b6f51df6afaf507685add148f6ab6',
    '0xcbffcb2c38ecd19468d366d392ac0c1dc7f04bb6',
    '0xe9172daf64b05b26eb18f07ac8d6d723acb48f99',
    '0xe95f6604a591f6ba33accb43a8a885c9c272108c',
    '0xf51cd688b8744b1bfd2fba70d050de85ec4fb9fb',
    '0xf59869753f41db720127ceb8dbb8afaf89030de4',
    '0xf7858da8a6617f7c6d0ff2bcafdb6d2eedf64840',
    ]
    
    huobi_addresses = [
    '0x0511509A39377F1C6c78DB4330FBfcC16D8A602f',
    '0x1205E4f0D2f02262E667fd72f95a68913b4F7462',
    '0x18709e89bd403f470088abdacebe86cc60dda12e',
    '0x30741289523c2e4d2a62c7d6722686d14e723851',
    '0x5c985e89dde482efe97ea9f1950ad149eb73829b',
    '0xa929022c9107643515f5c777ce9a910f0d1e490c',
    '0xc589b275e60dda57ad7e117c6dd837ab524a5666',
    '0xcac725bef4f114f728cbcfd744a731c2a463c3fc',
    '0xd70250731a72c33bfb93016e3d1f0ca160df7e42',
    '0xe195b82df6a797551eb1acd506e892531824af27',
    '0xe4818f8fde0c977a01da4fa467365b8bf22b071e',
    ]

    coinbase_addresses = [
    "0x71660c4005BA85c37ccec55d0C4493E66Fe775d3",
    "0x503828976D22510aad0201ac7EC88293211D23Da",
    "0xddfAbCdc4D8FfC6d5beaf154f18B778f892A0740",
    "0x3cD751E6b0078Be393132286c442345e5DC49699",
    "0xb5d85CBf7cB3EE0D56b3bB207D5Fc4B82f43F511",
    "0xeB2629a2734e272Bcc07BDA959863f316F4bD4Cf",
    "0xD688AEA8f7d450909AdE10C47FaA95707b0682d9",
    "0x02466E547BFDAb679fC49e96bBfc62B9747D997C",
    "0x6b76F8B1e9E59913BfE758821887311bA1805cAB",
    "0xA9D1e08C7793af67e9d92fe308d5697FB81d3E43",
    "0x77696bb39917C91A0c3908D577d5e322095425cA",
    "0x66c57bF505A85A74609D2C83E94Aabb26d691E1F",
    "0x95A9bd206aE52C4BA8EecFc93d18EACDd41C88CC",
    "0xb739D0895772DBB71A89A3754A160269068f0D45"
    ]

    kraken_addresses = [
    "0x2910543Af39abA0Cd09dBb2D50200b3E800A63D2",
    "0x0A869d79a7052C7f1b55a8EbAbbEa3420F0D1E13",
    "0xE853c56864A2ebe4576a807D26Fdc4A0adA51919",
    "0x267be1C1D684F78cb4F6a176C4911b741E4Ffdc0",
    "0xFa52274DD61E1643d2205169732f29114BC240b3",
    "0x53d284357ec70cE289D6D64134DfAc8E511c8a3D",
    "0x89e51fA8CA5D66cd220bAed62ED01e8951aa7c40",
    "0xc6bed363b30df7f35b601a5547fe56cd31ec63da",
    "0x29728D0efd284D85187362fAA2d4d76C2CfC2612",
    "0xAe2D4617c862309A3d75A0fFB358c7a5009c673F",
    "0x43984D578803891dfa9706bDEee6078D80cFC79E",
    "0x66c57bF505A85A74609D2C83E94Aabb26d691E1F",
    "0xDA9dfA130Df4dE4673b89022EE50ff26f6EA73Cf",
    "0xA83B11093c858c86321FBc4c20FE82cdbd58E09E",
    "0x2e7542eC36df6429D8C397F88C4Cf0c925948f44",
    "0xa24787320ede4CC19D800bf87B41Ab9539c4dA9D",
    "0xe9f7eCAe3A53D2A67105292894676b00d1FaB785"
    ]

    # Create a new column to identify addresses that received tokens from a CEX and identify CEXs
    accumulators["received_from_cex"] = '-'
    accumulators["is_a_cex"] = '-'
    
    response = requests.get(f'https://api.syve.ai/v1/filter-api/erc20?eq:token_address={token_address}&size=100000')
    transfers_data = response.json()
    token_transfers = create_dataframe(transfers_data)
    
    # Boolean Mask for transfers from CEX
    combined_addresses = binance_addresses + crypto_com_address + kucoin_addresses + okx_addresses + huobi_addresses + coinbase_addresses + kraken_addresses
    token_transfers = token_transfers[token_transfers["from_address"].isin(combined_addresses)]
    
    # Identify addresses that have received tokens from a CEX
    for address in accumulators["from_address"]:
        if address in token_transfers["to_address"].values:
            accumulators.loc[accumulators["from_address"] == address, "received_from_cex"] = 'Yes'
    
    # Identify addresses that have received tokens from a CEX
    for address in accumulators["from_address"]:
        if address in combined_addresses:
            accumulators.loc[accumulators["from_address"] == address, "is_a_cex"] = 'Yes'
            
def create_received_from_dex_labels(time_days, token_address='0xf21661d0d1d76d3ecb8e1b9f1c923dbfffae4097'):
    
    current_time = datetime.now()
    start_time = current_time - timedelta(days=time_days)
    unix_time = int(start_time.timestamp())
    
    accumulators["received_from_dex"] = '-'
    
    response = requests.get(f'https://api.syve.ai/v1/filter-api/dex-trades?eq:token_address={token_address}&gt:timestamp={unix_time}&size=100000')
    trades_data = response.json()
    token_trades = create_dataframe(trades_data)
    
    # Boolean Mask for DEX Trades
    token_trades = token_trades[token_trades["trader_address"].isin(accumulators["from_address"].values)]
    
    for address in accumulators["from_address"]:
        if address in token_trades["trader_address"].values:
            accumulators.loc[accumulators["from_address"] == address, "received_from_dex"] = 'Yes'       
            
def create_accummulators(token_address='0xf21661d0d1d76d3ecb8e1b9f1c923dbfffae4097'):
    
    global accumulators, fresh_wallets
    
    print('Started')
    sql_api = 'https://api.syve.ai/v1/sql'
    filter_api = 'https://api.syve.ai/v1/filter-api/transactions?eq:from_address='
    headers = {"Content-Type": "application/json"}

    in_query = {
        "query" : f"SELECT SUM(value_token) AS tokens_in, to_address AS address FROM eth_erc20 WHERE token_address = '{token_address}' AND timestamp > NOW() - INTERVAL 7 days GROUP BY 2 ORDER BY 1 DESC"
    }

    out_query = {
        "query" : f"SELECT SUM(value_token) AS tokens_out, from_address AS address FROM eth_erc20 WHERE token_address = '{token_address}' AND timestamp > NOW() - INTERVAL 7 days GROUP BY 2 ORDER BY 1 DESC"
    }

    # Get API responses
    in_data = get_sql_api_response(sql_api, headers, in_query)
    out_data = get_sql_api_response(sql_api, headers, out_query)
    
    # Create dataframes
    df_in = create_dataframe(in_data)
    df_out = create_dataframe(out_data)

    # Calculate changes and sort dataframe
    accumulators = create_accumulators_table(df_in, df_out)
    
    # No numbers in scientific notation & change column order
    pd.set_option('display.float_format', '{:f}'.format)
    accumulators = accumulators[["from_address", "tokens_in", "tokens_out", "Accumulated"]]
    
    # Label fresh wallets in accumulators
    create_cex_labels(token_address=token_address)
    create_received_from_dex_labels(7, token_address=token_address)
    
    return accumulators

# Streamlit UI
with st.echo(code_location='below'):
    
    # Replace with the necessary inputs for your script
    token_address = st.text_input("Enter token address", "0xd084944d3c05cd115c09d072b9f44ba3e0e45921")
    
    # Call your main function and get the results
    create_accummulators(token_address)
    
    # Display results
    st.write("Accumulators Over the Past 7 Days")
    st.write(accumulators)

    create_fresh_wallets_df(accumulators, filter_api)
    label_fresh_wallets()
    
    st.write("Fresh Wallets Over the Past 7 Days")
    st.write(fresh_wallets)
