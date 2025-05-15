# Copyright 2025 The go-ethereum Authors
# This file is part of the go-ethereum library.
#
# The go-ethereum library is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# The go-ethereum library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

# Parse log file into a pandas dataframe.
# Example usage:
# python geth-log-plot.py path/to/logfile.log

# Example log format:
# INFO [05-05|01:02:24.528] Imported new potential chain segment     number=22,413,537 hash=f30523..3aa189 blocks=1  txs=202  mgas=13.965  elapsed=71.839ms   mgasps=194.390 blobs=0 agems=1528    snapdiffs=8.98MiB triediffs=218.11MiB triedirty=240.97MiB
# INFO [05-05|01:02:24.640] Transaction known by                     block=f30523..3aa189 type=0 tx=eac45c..d08a6f size=343   blobs=0 have=false havesize=0       peers=2 knows=0 since=0
# INFO [05-05|01:02:24.640] Transaction known by                     block=f30523..3aa189 type=2 tx=315a0c..464f34 size=5928  blobs=0 have=false havesize=0       peers=2 knows=0 since=0
# INFO [05-08|09:28:48.958] Transaction known by                     block=f52321..836b0c type=2 tx=2e211f..b57edf size=178   blobs=0 have=true  havesize=178     peers=50 knows=50 since=5486    txpoolsize=41545
# INFO [05-08|09:28:48.958] Tx timing                                tx=2e211f..b57edf type=2 TxSend="[5485 5485 5485 5485 5485 5485 5485]"  AnnSend=[] TxRecv="[5486 5422 5409 5379]"                  AnnRecv="[5479 5465 5465 5459 5458 5444 5440 5421 5407 5397 5386 5385 5378 5378 5371 5369 5365 5364 5358 5350 5344 5339 5324 5323 5309 5300 5266 5240 5236 5219 5213 5024 4687 3959]"
# INFO [05-08|09:28:48.958] Transaction known by                     block=f52321..836b0c type=2 tx=20707b..ea64fc size=177   blobs=0 have=true  havesize=177     peers=50 knows=44 since=212,409 txpoolsize=41544
# INFO [05-08|09:28:48.958] Tx timing                                tx=20707b..ea64fc type=2 TxSend="[212408 212408]"                       AnnSend=[] TxRecv="[212409 212373 212360 212341 212340 212323 212321 212286 211537]" AnnRecv="[212420 212416 212416 212415 212411 212403 212401 212386 212379 212376 212335 212318 212298 212287 212283 212266 212259 212193 212163 211722 211586 210335 207682 197985 22795 11870 11757 11706 11683 11656 11550]"

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import datetime
import re
import sys
import os
import argparse
import numpy as np
import seaborn as sns

def parse_log_file(log_file):
    # regex to match the log lines
    log_line_regex = re.compile(r'(\w+)\s+\[(\d+-\d+)\|(\d+:\d+:\d+\.\d+)\]\s+(.*)')

    # list to hold the parsed log lines
    log_lines = []

    # open the log file and read it line by line
    with open(log_file, 'r') as f:
        for line in f:
            match = log_line_regex.match(line)
            if match:
                level = match.group(1)
                date_str = match.group(2)
                time_str = match.group(3)
                message = match.group(4)
                timestamp_str = f"2025-{date_str} {time_str}"
                timestamp = datetime.datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S.%f')
                log_lines.append((timestamp, level, message))

    return log_lines

def create_dataframes(log_lines):
    # create a pandas dataframe from the log lines
    df = pd.DataFrame(log_lines, columns=['timestamp', 'level', 'message'])

    # convert the timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # select lines for blocks
    blocks = df[df['message'].str.contains('Imported new')].copy()
    # extract the block short hash from the message
    blocks['block'] = blocks['message'].str.extract(r'hash=([0-9a-f.]{14})')
    # extract the block number from the message
    blocks['number'] = blocks['message'].str.extract(r'number=(\d+)').astype(int)
    # extract the block size from the message
    blocks['size'] = blocks['message'].str.extract(r'txs=(\d+)').astype(int)
    # extract the block gas from the message
    blocks['mgas'] = blocks['message'].str.extract(r'mgas=(\d+\.\d+)').astype(float)
    # extract the block elapsed time from the message
    blocks['elapsed'] = blocks['message'].str.extract(r'elapsed=(\d+\.\d+)ms').astype(float)
    # extract the block mgasps from the message
    blocks['mgasps'] = blocks['message'].str.extract(r'mgasps=(\d+\.\d+)').astype(float)
    # extract the block blobs from the message
    blocks['blobs'] = blocks['message'].str.extract(r'blobs=(\d+)').astype(int)
    # extract the block agems from the message
    blocks['agems'] = blocks['message'].str.extract(r'agems=(\d+)').astype(int)

    # select only lines with "Transaction known by", drop the rest
    btxs = df[df['message'].str.contains('Transaction known by')].copy()
    # extract the transaction short hash from the message
    btxs['tx'] = btxs['message'].str.extract(r'tx=([0-9a-f.]{14})')

    # extract the block short hash from the message
    btxs['block'] = btxs['message'].str.extract(r'block=([0-9a-f.]{14})')

    # # extract the provenance from the message
    # df['provenance'] = df['message'].str.extract(r'provenance=([0-9a-f]{64})')
 
    # extract value from type=2 tx=22272e..e3d631 have=true peers=36 knows=36
    btxs['type'] = btxs['message'].str.extract(r'type=(\d+)')
    btxs['have'] = btxs['message'].str.extract(r'have=(\w+)').isin(['true', 'True'])
    btxs['peers'] = btxs['message'].str.extract(r'peers=(\d+)').astype(int)
    btxs['knows'] = btxs['message'].str.extract(r'knows=(\d+)').astype(int)
    btxs['blobs'] = btxs['message'].str.extract(r'blobs=(\d+)').astype(int)
    btxs['havesize'] = btxs['message'].str.extract(r'havesize=(\d+)').astype(int)
    btxs['since'] = btxs['message'].str.extract(r'since=(\d+)').astype(float) /1000 * -1
    btxs['size'] = btxs['message'].str.extract(r'size=(\d+)').astype(int)

    btxs['da'] = btxs['knows'] / btxs['peers']

    # extract transaction timing from the message

    # select only lines with "Transaction known by", drop the rest
    btxtime = df[df['message'].str.contains('Tx timing')].copy()
    btxtime['tx'] = btxtime['message'].str.extract(r'tx=([0-9a-f.]{14})')
    btxtime['type'] = btxtime['message'].str.extract(r'type=(\d+)').astype(int)
    btxtime['TxSend'] = btxtime['message'].str.extract(r'TxSend="?\[(.*?)\]"?')
    btxtime['TxSend'] = btxtime['TxSend'].str.split().apply(lambda x: [int(i) for i in x])
    btxtime['TxRecv'] = btxtime['message'].str.extract(r'TxRecv="?\[(.*?)\]"?')
    btxtime['TxRecv'] = btxtime['TxRecv'].str.split().apply(lambda x: [int(i) for i in x])
    btxtime['AnnSend'] = btxtime['message'].str.extract(r'AnnSend="?\[(.*?)\]"?')
    btxtime['AnnSend'] = btxtime['AnnSend'].str.split().apply(lambda x: [int(i) for i in x])
    btxtime['AnnRecv'] = btxtime['message'].str.extract(r'AnnRecv="?\[(.*?)\]"?')
    btxtime['AnnRecv'] = btxtime['AnnRecv'].str.split().apply(lambda x: [int(i) for i in x])

    btxtime['peers'] = btxtime['message'].str.extract(r'peers=(\d+)').astype(int)
    btxtime['knows'] = btxtime['message'].str.extract(r'knows=(\d+)').astype(int)
    btxtime['blobs'] = btxtime['message'].str.extract(r'blobs=(\d+)').astype(int)
    btxtime['havesize'] = btxtime['message'].str.extract(r'havesize=(\d+)').astype(int)
    btxtime['since'] = btxtime['message'].str.extract(r'since=(\d+)').astype(float) /1000 * -1
    btxtime['size'] = btxtime['message'].str.extract(r'size=(\d+)').astype(int)

    # remove the message columns
    btxs.drop(columns=['message'], inplace=True)
    btxtime.drop(columns=['message'], inplace=True)
    blocks.drop(columns=['message'], inplace=True)

    return btxs, blocks, btxtime

def plot_dataframe(df):
    # set the timestamp as the index
    df.set_index('timestamp', inplace=True)

    # # plot the number of transactions over time
    # df.resample('1T').count()['tx'].plot(ax=ax, label='Transactions', color='blue')

    # # plot the number of provenance over time
    # df.resample('1T').count()['provenance'].plot(ax=ax, label='Provenance', color='orange')

    # set the title and labels
    # ax.set_title('Transactions and Provenance Over Time')
    # ax.set_xlabel('Time')
    # ax.set_ylabel('Count')

    # format the x-axis to show the date and time
    # ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M:%S'))
    # plt.xticks(rotation=45)

    # plot overall ratio of transactions per type in a pie chart with seaborn
    fig, ax = plt.subplots(figsize=(6, 6))
    df1 = df[['type']].groupby('type').size()
    ax.pie(df1, labels=df1.index, autopct='%1.1f%%', startangle=90)
    ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    # add legend
    labels = [f'{l}, {s:0.1f}%' for l, s in zip(df1.index, df1.values/df1.sum()*100)]
    ax.legend(labels, title="Transaction types", loc="upper right")
    ax.set_title('Overall ratio of transactions per type')
    plt.savefig('geth_tx_type_ratio.png')

    # plot the same, now weighted by transaction size
    fig, ax = plt.subplots(figsize=(6, 6))
    df1 = df[['type','realSize']].groupby('type').sum()
    ax.pie(df1['realSize'], labels=df1.index, autopct='%1.1f%%', startangle=90)
    ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    # add legend
    labels = [f'{l}, {s:0.1f}%' for l, s in zip(df1.index, df1['realSize'].values/df1['realSize'].sum()*100)]
    ax.legend(labels, title="Transaction types", loc="upper right")
    ax.set_title('Overall ratio of transactions per type, weighted by transaction size')
    plt.savefig('geth_tx_type_ratio_weighted.png')


    df['peersCat'] = pd.cut(df['peers'], bins=[1, 5, 10, 15 ,25, 50, 100, 200, 300, 400, 500], 
                             labels=[5, 10, 15 ,25, 50, 100, 200, 300, 400, 500])


    # plot da over time
    fig, ax = plt.subplots(figsize=(12, 6))
    df1 = df[df['public']]
    df1 = df1[['da','type']].groupby('type').resample('6.4min', include_groups=False).mean()
    sns.lineplot(data=df1, x='timestamp', y='da', hue='type',
                  ax=ax)
    ax.set_title('Average ratio of peers knowing transactions of a given type, over time (epochs)')
    ax.set_xlabel('Time')
    ax.set_ylabel('Ratio of peers knowing a "block transaction"')
    ax.legend()
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M:%S'))
    plt.xticks(rotation=45)
    plt.savefig('geth_da_over_time.png')

    # plot da over peercount
    fig, ax = plt.subplots(figsize=(12, 6))
    df1 = df[df['public']]
    # df1 = df1[df1['peers'].isin([1,2,3,5,7,10,20,30,40,50,60,70,80,90,100,120,140,160,180,200,
    #                              300, 400, 500, 600, 700, 800, 900, 1000])]
    df1 = df1[['da','peers','peersCat','type']].groupby(['type','peersCat']).mean()
    sns.lineplot(data=df1, x='peersCat', y='da', hue='type',
                  ax=ax)
    ax.set_title('Average ratio of peers knowing transactions of a given type, as a function of peer count')
    ax.set_xlabel('Peer count')
    ax.set_ylabel('Ratio of peers knowing a "block transaction"')
    ax.legend()
    plt.savefig('geth_da_over_peercount.png')

    # plot public txs over time
    fig, ax = plt.subplots(figsize=(12, 6))
    df1 = df[['public','type']].groupby('type').resample('6.4min', include_groups=False).apply(lambda x: np.sum(x)/len(x))
    sns.lineplot(data=df1, x='timestamp', y='public', hue='type',
                  ax=ax)
    ax.set_title('Average ratio of public block transactions of a given type, over time (epochs)')
    ax.set_xlabel('Time')
    ax.set_ylabel('Ratio of public "block transaction"')
    ax.legend()
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M:%S'))
    plt.xticks(rotation=45)
    plt.savefig('geth_public_over_time.png')

    # plot public txs over peercount
    fig, ax = plt.subplots(figsize=(12, 6))
    df1 = df
    # df1 = df1[df1['peers'].isin([1,2,3,5,7,10,20,30,40,50,60,70,80,90,100,120,140,160,180,200,
    #                              300, 400, 500, 600, 700, 800, 900, 1000])]
    df1 = df1[['public','peersCat','type']].groupby(['type','peersCat']).apply(lambda x: np.sum(x)/len(x))
    sns.lineplot(data=df1, x='peersCat', y='public', hue='type',
                  ax=ax)
    ax.set_title('Average ratio of public block transactions of a given type, as a function of peer count')
    ax.set_xlabel('Peer count')
    ax.set_ylabel('Ratio of public "block transaction"')
    ax.legend()
    plt.savefig('geth_public_over_peercount.png')

    # plot public txs ratio with columns, showing also the values for each bar
    fig, ax = plt.subplots(figsize=(12, 6))
    df1 = df[['public','type']].groupby('type').apply(lambda x: np.sum(x)/len(x))
    sns.barplot(data=df1, x='type', y='public', hue='type',
                  ax=ax)
    ax.set_title('Average ratio of public block transactions of a given type')
    ax.set_xlabel('"Block transaction" type')
    ax.set_ylabel('Ratio of public "block transaction"')
    ax.legend()
    for p in ax.patches:
        ax.annotate(f'{p.get_height()*100:.1f}%', (p.get_x() + p.get_width() / 2., p.get_height()),
                    ha='center', va='center', rotation=0, fontsize=12, color='black',
                    xytext=(0, 5), textcoords='offset points')
    plt.savefig('geth_public.png')

    # plot received txs over time
    fig, ax = plt.subplots(figsize=(12, 6))
    df1 = df[['have','type']].groupby('type').resample('6.4min', include_groups=False).apply(lambda x: np.sum(x)/len(x))
    sns.lineplot(data=df1, x='timestamp', y='have', hue='type',
                  ax=ax)
    ax.set_title('Average ratio of received block transactions of a given type, over time (epochs)')
    ax.set_xlabel('Time')
    ax.set_ylabel('Ratio of received "block transaction"')
    ax.legend()
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M:%S'))
    plt.xticks(rotation=15)
    plt.savefig('geth_received_over_time.png')

    df1 = df[['have','peersCat','type']].groupby(['type','peersCat']).apply(lambda x: np.sum(x)/len(x))
    sns.lineplot(data=df1, x='peersCat', y='have', hue='type',
                  ax=ax)
    ax.set_title('Average ratio of received block transactions of a given type, as a function of peer count')
    ax.set_xlabel('Peer count')
    ax.set_ylabel('Ratio of received "block transaction"')
    ax.legend()
    plt.savefig('geth_received_over_peercount.png')

    # plot received block txs ratio with columns
    fig, ax = plt.subplots(figsize=(12, 6))
    df1 = df[['have','type']].groupby('type').apply(lambda x: np.sum(x)/len(x))
    sns.barplot(data=df1, x='type', y='have', hue='type',
                  ax=ax)
    ax.set_title('Average ratio of received transactions of a given type')
    ax.set_xlabel('"Block transaction" type')
    ax.set_ylabel('Ratio of received "block transaction"')
    ax.legend()
    for p in ax.patches:
        ax.annotate(f'{p.get_height()*100:.1f}%', (p.get_x() + p.get_width() / 2., p.get_height()),
                    ha='center', va='center', rotation=0, fontsize=12, color='black',
                    xytext=(0, 5), textcoords='offset points')
    plt.savefig('geth_received.png')

    # plot seen, but not received block txs ratio with columns
    fig, ax = plt.subplots(figsize=(12, 6))
    df1 = df[['onlyPeers','type']].groupby('type').apply(lambda x: np.sum(x)/len(x))
    sns.barplot(data=df1, x='type', y='onlyPeers', hue='type',
                  ax=ax)
    ax.set_title('Average ratio of seen but not received block transactions of a given type')
    ax.set_xlabel('"Block transaction" type')
    ax.set_ylabel('Ratio of seen but not received "block transaction"')
    ax.legend()
    plt.savefig('geth_onlyPeers.png')

    # plot seen, but not received block txs over peercount
    fig, ax = plt.subplots(figsize=(12, 6))
    df1 = df
    df1 = df1[df1['peers'].isin([1,2,3,5,7,10,20,30,40,50,60,70,80,90,100,120,140,160,180,200,
                                 300, 400, 500, 600, 700, 800, 900, 1000])]
    df1 = df1[['onlyPeers','peers','type']].groupby(['type','peers']).apply(lambda x: np.sum(x)/len(x))
    sns.lineplot(data=df1, x='peers', y='onlyPeers', hue='type',
                  ax=ax)
    ax.set_title('Average ratio of seen but not received block transactions of a given type, as a function of peer count')
    ax.set_xlabel('Peer count')
    ax.set_ylabel('Ratio of seen but not received "block transaction"')
    ax.legend()
    plt.savefig('geth_onlyPeers_over_peercount.png')

    # --------------

    # plot histogram of since values per type using Seaborn
    fig, ax = plt.subplots(figsize=(12, 6))
    sns.histplot(data=df[df['have'] & (df['since']>=-32*12)],
                    #kind='hist',
                    #log_scale=(True, False),
                    x='since', hue='type',
                    hue_order=["0","1","2","3","4"],
                    stat="proportion", common_norm=False, # independent density normalization
                    bins=100,
                    linewidth=0,
                    kde=True,
                    kde_kws=dict(bw_adjust=0.5),
                    line_kws=dict(linewidth = 2),
                    ax=ax)
    ax.set_title('Transaction age in the mempool before block inclusion')
    ax.set_xlabel('Time since transaction was received (s)')
    ax.set_ylabel('Portion of transactions')
    fig.savefig('geth_btx_age_hist.png')


    # show the plot
    # plt.tight_layout()
    # plt.show()

    # create a new figure for the histogram
    fig, ax = plt.subplots(figsize=(6, 6))
    # plot the histogram of the DA values
    df['da'].hist(bins=100, density=False).plot(ax=ax, label='DA', color='red')
    #df['da'].hist(bins=1000, cumulative=True, density=True, histtype='step').plot(ax=ax, label='DA', color='red')
    # set the title and labels
    ax.set_title('Histogram of DA values')
    ax.set_xlabel('EL DA')
    ax.set_ylabel('Probability')
    # add a legend
    ax.legend()
    # save the histogram to a file
    plt.tight_layout()
    plt.savefig('geth_da_hist.png')

    # replot the same using Seaborn
    fig, ax = plt.subplots(figsize=(12, 6))
    sns.histplot(data=df[df['da']!=0.0], x='da', hue='type',
                    stat="proportion", common_norm=False, # independent density normalization
                    bins=100,
                    multiple="dodge",
                    hue_order=["0","1","2","3","4"],
                    #log_scale=(False, True)
                    element="step", fill=True,
                    #kde=True,
                    ax=ax)
    ax.set_title('Transaction availability in the mempool')
    ax.set_xlabel('Level of dissusion (based on sampling)')
    ax.set_ylabel('Portion of transactions')
    plt.savefig('geth_da_hist_seaborn.png')

    fig, ax = plt.subplots(figsize=(12, 6))
    sns.ecdfplot(data=df[df['da']!=0.0], x='da', hue='type',
                 #stat="density", common_norm=False, # independent density normalization
                 #bins=100, multiple="dodge",
                 #kde=True,
                 ax=ax)
    plt.savefig('geth_da_ecdf.png')

def plot_getblobs_statistics(btxs):
    # plot the ratio of blocks where we have all type 3 transactions
    blobtxs = btxs[btxs['type'] == "3"]
    blobtxs['haveblobs'] = np.where(blobtxs['have'], blobtxs['blobs'], 0)

    #blobtxs[['block', 'tx', 'have', 'public', 'blobs', 'haveblobs']].groupby('block').apply(print)
    print(blobtxs[['block', 'tx', 'have', 'public', 'blobs', 'haveblobs']])

    # sum the number of blobs per block'; use all on have and public columns
    blobstats=blobtxs[['block','have','public','blobs', 'haveblobs']].groupby('block').agg({
        'blobs': 'sum',
        'have': 'all',
        'public': 'all',
        'haveblobs': 'sum',
        }).reset_index()
    blobstats['missblobs'] = blobstats['blobs'] - blobstats['haveblobs']

    # get the category of the block based on the have and public columns    
    blobstats['category'] = blobstats[['public','have']].apply(tuple, axis=1).map({
        (False,False): 'Private',
        (False,True): 'Private',
        (True,True): 'getBlobs works',
        (True,False): 'getBlobs fails',
        })
    
    blobstats['blobs'] = blobstats['blobs'].astype(int)


    print(blobtxs[~blobtxs['have']][['block', 'tx', 'have', 'public', 'blobs', 'haveblobs']])
    print(blobstats[blobstats['missblobs']>0])

    ## plot the category in stacked hist chart, per number of blobs
    fig, ax = plt.subplots(figsize=(12, 6))
    sns.histplot(data=blobstats, x='blobs', hue='category',
                bins=[0.5,1.5,2.5,3.5,4.5,5.5,6.5,7.5,8.5,9.5],
                multiple="stack",
                hue_order=["Private", "getBlobs fails","getBlobs works"],
                legend=True,
                ax=ax)
    # ax.legend(title='Category', loc='upper right',
    #           labels=['all public, and getBlobs works', 'all public, but getBlobs fails', 'some Private'])
    ax.set_title('Blobcount vs. getBlobs effectiveness')
    ax.set_xlabel('Number of blobs in the block')
    ax.set_ylabel('Number of Private/Public blocks with a given blobcount')
    ax.set_xticks(np.arange(1, 10, 1))
    # save the histogram to a file
    plt.tight_layout()
    plt.savefig('geth_blobtx_category.png')


    # plot the category in pie chart
    # make sure the right colors are used
    fig, ax = plt.subplots(figsize=(6, 6))
    piedf = blobstats['category'].value_counts()
    print(piedf)
    piedf.plot.pie(autopct='%1.1f%%', startangle=90, ax=ax,
            colors=piedf.index.map({
                'Private': 'blue',
                'getBlobs fails': 'orange',
                'getBlobs works': 'green'
            })
    )
    ax.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    ax.set_title('getBlobs effectiveness')
    ax.set_ylabel('')  # remove the y-label
    # ax.legend(title='Category', loc='upper right', labels=['some Private', 'all public, but getBlobs fails', 'all public, and getBlobs works'])
    plt.tight_layout()
    plt.savefig('geth_blobtx_have_all.png')

    # focusing on private blocks, plot haveblobs as a functions of blobs in stacked histogram
    fig, ax = plt.subplots(figsize=(12, 6))
    sns.histplot(data=blobstats[blobstats['category'] == 'Private'], x='blobs', hue='missblobs',
                bins=[0.5,1.5,2.5,3.5,4.5,5.5,6.5,7.5,8.5,9.5],
                multiple="stack",
                palette="tab10",
                # hue_order=["False", "True"],
                legend=True,
                ax=ax)
    # ax.legend(title='Category', loc='upper right',
    #           labels=['all public, and getBlobs works', 'all public, but getBlobs fails', 'some Private'])
    ax.set_title('Blobcount vs. missing blobs')
    ax.set_xlabel('Number of blobs in the block')
    ax.set_ylabel('Number of Private blocks with a given blobcount\n and the number of blobs we miss')
    ax.set_xticks(np.arange(1, 10, 1))
    # save the histogram to a file
    plt.tight_layout()
    plt.savefig('geth_blobtx_private.png')

    exit(0)

def plot_transaction_timing(btxtime):
    # plot the transaction timing
    color_by_type = { 0: 'blue', 1: 'orange', 2: 'green', 3: 'red', 4: 'purple' }

    fig1, ax1 = plt.subplots(figsize=(12, 6)) # block rx timing based
    fig2, ax2 = plt.subplots(figsize=(12, 6)) # firstrx timing based
    # plot the AnnRecv times
    plottedtype = {}
    plotted = 0
    allpoints = []

    print(btxtime)
    for i, row in btxtime[::-1].iterrows():
        annrecv = np.array(row['AnnRecv'])
        txrecv = np.array(row['TxRecv'])
        recv = np.concatenate((txrecv, annrecv)) /1000
        type = row['type']
        txid = row['tx']
        label = f'{txid}, type={type}'
        if len(recv) > 1 and row['peers']>45: # and type != 0:
            recv *= -1
            recv = np.sort(recv)
            recv_firstrx = recv - recv.min()
            maxrecvfrom = row['peers'] - len(row['TxSend'])
            #print(f'{txid} {row["knows"]}/{row["peers"]}: {len(recv)} =? {len(annrecv)}+{len(txrecv)}+{len(row["TxSend"])} ({len(row["AnnSend"])})')
            #y = [(x) / (len(recv)-1) for x in range(len(recv))]
            y = [x/maxrecvfrom for x in range(len(recv))]
            points = pd.DataFrame({ 'type': type, 'delay': recv_firstrx, 'q': y})
            quantized = points.quantile((np.arange(100)+1)/100)
            quantized['type'] = type # reset type to integer
            quantized['blobs'] = row['blobs']
            #allpoints = pd.concat([allpoints, quantized], ignore_index=True, axis=0)
            allpoints.append(quantized)

            if type not in plottedtype:
                plottedtype[type] = 0
            if plottedtype[type] < 4:
                ax1.plot(recv, y, label=label, color=color_by_type[type])
                ax2.plot(recv_firstrx, y, label=label, color=color_by_type[type])
                plottedtype[type] += 1
            plotted += 1
            # if plotted >= 1000:
            #     break
            # # add a vertical line for the transaction time
            # ax.axvline(x=row['timestamp'], color='red', linestyle='--')
    # set the title and labels
    ax1.set_title('Transaction availability in the mempool before block inclusion')
    ax1.set_xlabel('Time before the arrival of including block (s)')
    ax1.set_ylabel('Portion of peers announced the transaction to us')
    ax1.legend()
    ax1.set_ylim(0, 1)    
    ax1.set_xlim(-20, 0)
    fig1.savefig('geth_tx_timing.png')

    ax2.set_title('Transaction availability in the mempool')
    ax2.set_xlabel('Time after first reception or announcement (s)')
    ax2.set_ylabel('Portion of peers announced the transaction to us')
    ax2.legend()
    ax2.set_ylim(0, 1)    
    ax2.set_xlim(0, 20)
    fig2.savefig('geth_tx_timing_firstrx.png')

    allpoints = pd.concat(allpoints, axis=0)
    fig, ax = plt.subplots(figsize=(12, 6))
    sns.lineplot(allpoints, x='delay', y=allpoints.index, orient='y', hue='type',
                #errorbar="sd",
                palette="tab10",
                ax=ax)
    ax.set_title('Transaction availability in the mempool')
    ax.set_xlabel('Time after first reception or announcement (s)')
    ax.set_ylabel('Portion of peers announced the transaction to us')    
    ax.set_ylim(0, 1)    
    ax.set_xlim(0, 20)
    plt.savefig('geth_tx_timing_2D.png')

    fig, ax = plt.subplots(figsize=(12, 6))
    type3 = allpoints[allpoints['type'] == 3]
    print(type3)
    sns.lineplot(type3, x='delay', y=type3.index, orient='y', hue='blobs',
                #errorbar="sd",
                ax=ax)
    ax.set_title('Blob transaction spreading in the mempool')
    ax.set_xlabel('Time after first reception or announcement (s)')
    ax.set_ylabel('Portion of peers announced the transaction to us')
    ax.set_ylim(0, 1)    
    ax.set_xlim(0, 20)
    plt.savefig('geth_blobtx_timing_2D.png')

def main():
    # create the argument parser
    parser = argparse.ArgumentParser(description='Parse and plot Geth log file.')
    parser.add_argument('log_file', type=str, help='Path to the Geth log file')
    args = parser.parse_args()

    # check if the log file exists
    if not os.path.exists(args.log_file):
        print(f"Log file {args.log_file} does not exist.")
        sys.exit(1)

    # parse the log file
    log_lines = parse_log_file(args.log_file)

    # create a dataframe from the log lines
    df, blocks, btxtime = create_dataframes(log_lines)

    # calculate the real size of the transactions
    df['realSize'] = df['size'] + df['blobs'] * 131175 + 9

    # mark public transactions: those that we have or that are known by at least one peer
    df['public'] = df['have'] | (df['da'] > 0.0)
    df['onlyPeers'] = (~ df['have']) & (df['da'] > 0.0)
    df['onlyUs'] = df['have'] & (df['da'] == 0.0)

    print(df)
    print(df[['public','have','onlyPeers','onlyUs','type']].groupby('type').sum())
    print(df[['public','have','onlyPeers','onlyUs','type','peers']].groupby(['type','peers']).sum())


    plot_getblobs_statistics(df)

    # plot the dataframe
    plot_dataframe(df)

    plot_transaction_timing(btxtime)

if __name__ == '__main__':
    main()
