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
# INFO [04-21|23:15:00.747] Transaction known by                     type=2 tx=22272e..e3d631 us=true  peers=36 knows=36
# WARN [04-21|23:15:00.747] Transaction provenance                   tx=22272e..e3d631 provenance=2c8087f9eafa8314fde8d07218efff9b57ac38663c7005ce187c2140cbfed21a
# INFO [04-21|23:15:00.747] Transaction known by                     type=2 tx=d8ed92..7a2758 us=true  peers=36 knows=36
# WARN [04-21|23:15:00.747] Transaction provenance                   tx=d8ed92..7a2758 provenance=10b52ae19496fc598949ab13b97ade999586cf405005d83cf2bab68ce2c69367
# INFO [04-21|23:15:00.747] Transaction known by                     type=2 tx=9e897a..b7ab5b us=true  peers=36 knows=36

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import datetime
import re
import sys
import os
import argparse
import numpy as np

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

    print(df['timestamp'])

    # extract the transaction hash from the message
    df['tx'] = df['message'].str.extract(r'tx=([0-9a-f]{64})')

    # extract the provenance from the message
    df['provenance'] = df['message'].str.extract(r'provenance=([0-9a-f]{64})')

    # select only lines with "Transaction known by", drop the rest
    df = df[df['message'].str.contains('Transaction known by')]
 
    # extract value from type=2 tx=22272e..e3d631 us=true  peers=36 knows=36
    df['type'] = df['message'].str.extract(r'type=(\d+)')
    df['us'] = df['message'].str.extract(r'us=(\w+)')
    df['peers'] = df['message'].str.extract(r'peers=(\d+)')
    df['knows'] = df['message'].str.extract(r'knows=(\d+)')
    df['da'] = df['knows'].astype(int) / df['peers'].astype(int)

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


    fig, ax = plt.subplots(figsize=(6, 6))
    # plot da over time with dots
    
    df['da'].plot(ax=ax, label='DA', color='red', line)
    ax.set_title('Average ratio of peers knowing a block transaction')
    ax.set_xlabel('Time')
    ax.set_ylabel('Ratio')
    ax.legend()
    plt.savefig('geth_da_over_time.png')
    print("Plot saved as geth_log_plot.png")


    #df['da'].resample('12min').mean().plot(ax=ax, label='DA', color='red')

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

    # plot the dataframe
    plot_dataframe(df)
if __name__ == '__main__':
    main()
