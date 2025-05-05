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
# INFO [04-21|23:15:00.747] Transaction known by                     type=2 tx=22272e..e3d631 have=true  peers=36 knows=36
# WARN [04-21|23:15:00.747] Transaction provenance                   tx=22272e..e3d631 provenance=2c8087f9eafa8314fde8d07218efff9b57ac38663c7005ce187c2140cbfed21a
# INFO [04-21|23:15:00.747] Transaction known by                     type=2 tx=d8ed92..7a2758 have=true  peers=36 knows=36
# WARN [04-21|23:15:00.747] Transaction provenance                   tx=d8ed92..7a2758 provenance=10b52ae19496fc598949ab13b97ade999586cf405005d83cf2bab68ce2c69367
# INFO [04-21|23:15:00.747] Transaction known by                     type=2 tx=9e897a..b7ab5b have=true  peers=36 knows=36

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
def create_dataframe(log_lines):
    # create a pandas dataframe from the log lines
    df = pd.DataFrame(log_lines, columns=['timestamp', 'level', 'message'])

    # convert the timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # extract the transaction hash from the message
    df['tx'] = df['message'].str.extract(r'tx=([0-9a-f]{64})')

    # extract the provenance from the message
    df['provenance'] = df['message'].str.extract(r'provenance=([0-9a-f]{64})')

    # select only lines with "Transaction known by", drop the rest
    df = df[df['message'].str.contains('Transaction known by')]
 
    # extract value from type=2 tx=22272e..e3d631 have=true  peers=36 knows=36
    df['type'] = df['message'].str.extract(r'type=(\d+)')
    df['have'] = df['message'].str.extract(r'have=(\w+)').isin(['true', 'True'])
    df['peers'] = df['message'].str.extract(r'peers=(\d+)').astype(int)
    df['knows'] = df['message'].str.extract(r'knows=(\d+)').astype(int)
    df['da'] = df['knows'] / df['peers']

    return df
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
    df1 = df1[df1['peers'].isin([1,2,3,5,7,10,20,30,40,50,60,70,80,90,100,120,140,160,180,200])]
    df1 = df1[['da','peers','type']].groupby(['type','peers']).mean()
    sns.lineplot(data=df1, x='peers', y='da', hue='type',
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
    df1 = df1[df1['peers'].isin([1,2,3,5,7,10,20,30,40,50,60,70,80,90,100,120,140,160,180,200])]
    df1 = df1[['public','peers','type']].groupby(['type','peers']).apply(lambda x: np.sum(x)/len(x))
    sns.lineplot(data=df1, x='peers', y='public', hue='type',
                  ax=ax)
    ax.set_title('Average ratio of public block transactions of a given type, as a function of peer count')
    ax.set_xlabel('Peer count')
    ax.set_ylabel('Ratio of public "block transaction"')
    ax.legend()
    plt.savefig('geth_public_over_peercount.png')

    # plot public txs ratio with columns
    fig, ax = plt.subplots(figsize=(12, 6))
    df1 = df[['public','type']].groupby('type').apply(lambda x: np.sum(x)/len(x))
    sns.barplot(data=df1, x='type', y='public', hue='type',
                  ax=ax)
    ax.set_title('Average ratio of public block transactions of a given type')
    ax.set_xlabel('"Block transaction" type')
    ax.set_ylabel('Ratio of public "block transaction"')
    ax.legend()
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
    plt.xticks(rotation=45)
    plt.savefig('geth_received_over_time.png')

    # plot received block txs ratio with columns
    fig, ax = plt.subplots(figsize=(12, 6))
    df1 = df[['have','type']].groupby('type').apply(lambda x: np.sum(x)/len(x))
    sns.barplot(data=df1, x='type', y='have', hue='type',
                  ax=ax)
    ax.set_title('Average ratio of received transactions of a given type')
    ax.set_xlabel('"Block transaction" type')
    ax.set_ylabel('Ratio of received "block transaction"')
    ax.legend()
    plt.savefig('geth_received.png')



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
    sns.ecdfplot(data=df[df['da']!=0.0], x='da', hue='type',
                 #stat="density", common_norm=False, # independent density normalization
                 #bins=100, multiple="dodge",
                 #kde=True,
                 ax=ax)
    # ax.set_title('Histogram of DA values')
    # ax.set_xlabel('EL DA')
    # ax.set_ylabel('Probability')
    # # add a legend
    # ax.legend()
    # # save the histogram to a file
    # plt.tight_layout()
    plt.savefig('geth_da_ecdf_seaborn.png')
    print("Histogram saved as geth_da_hist_seaborn.png")


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
    df = create_dataframe(log_lines)

    # mark public transactions: those that we have or that are known by at least one peer
    df['public'] = df['have'] | (df['da'] > 0.0)
    df['onlyPeers'] = (~ df['have']) & (df['da'] > 0.0)
    df['onlyUs'] = df['have'] & (df['da'] == 0.0)

    print(df)
    print(df[['public','have','onlyPeers','onlyUs','type']].groupby('type').sum())
    print(df[['public','have','onlyPeers','onlyUs','type','peers']].groupby(['type','peers']).sum())
    

    # plot the dataframe
    plot_dataframe(df)
if __name__ == '__main__':
    main()
