"""
owcell_network_report.py

This file will generate graphics for a given
network configuration using OMNeT++ SQLite results files.

TODO: Break up file into functions.
TODO: Query the database files instead of iterating over them.

Useful Links:
https://docs.omnetpp.org/tutorials/pandas/
https://docs.omnetpp.org/tutorials/tictoc/part6/
"""
import sqlite3
from sqlite3 import Error
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re
import statistics


def create_connection(db):
    """
    This function is utilized to connect to the OMNet++ results
    file stored as a SQLite file.
    """
    con = None
    try:
        con = sqlite3.connect(db)
    except Error as e:
        print(e)

    return con


def main():
    # Path for database to be opened.
    vec_database = '/share/test-#3-large.owcell.vec'
    sca_database = '/share/test-#3-large.owcell.sca'

    # Create connection to database.
    con = create_connection(vec_database)

    # Create cursor for querying runParam table,
    #   this contains the flow information we will use.
    # -------------------------------------------------------
    # | runId   | paramKey  | paramValue    | paramOrder    |
    # -------------------------------------------------------
    cur = con.execute('SELECT * FROM runParam')

    # Loop through the table and record the sizes sent
    #   and add up the time for each flow.
    info_rows = 0
    info_cols = 0
    info_racks = 0
    info_hosts = 0
    heat_dict = {}  # { (frm_cell, frm_rack, to_cell, to_rack) : ( size, length) }
    pattern = '.*\[(\d*)\]\..*\[(\d*)\].*\[(\d*)\].*\[(\d*)\].*'
    pattern2 = '.*\[(\d*)\]\..*\[(\d*)\].*\[(\d*)\].*'
    size = 0
    sizes_list = []
    lengths_list = []
    rates_list = []
    length = 1
    intra_cell = 0
    intra_rack = 0
    extra_cell = 0
    extra_rack = 0
    for row in cur:
        if 'sendBytes' in row[1]:
            size = int(row[2][:-3])
            sizes_list.append(size)
        elif 'tOpen' in row[1]:
            length = 1
            length += int(row[2][:-1])
        elif 'tSend' in row[1]:
            length += int(row[2][:-1])
        elif 'tClose' in row[1]:
            length += int(row[2][:-1])

            lengths_list.append(length)
            rates_list.append(size / length)
        elif 'connectAddress' in row[1]:
            capture = re.search(pattern, row[1])

            frm_cell = int(capture.group(1))
            frm_rack = int(capture.group(2))

            capture = re.search(pattern2, row[2])

            to_cell = int(capture.group(1))
            to_rack = int(capture.group(2))

            if (frm_cell, frm_rack, to_cell, to_rack) in heat_dict:
                old_size, old_len = heat_dict[(frm_cell, frm_rack, to_cell, to_rack)]
                heat_dict[(frm_cell, frm_rack, to_cell, to_rack)] = (old_size + size, old_len + length)
            else:
                heat_dict[(frm_cell, frm_rack, to_cell, to_rack)] = (size, length)

            if frm_cell == to_cell:
                intra_cell += size
            else:
                extra_cell += size

            if frm_cell == to_cell and frm_rack == to_rack:
                intra_rack += size
            else:
                extra_rack += size
        elif '**.rows' in row[1]:
            info_rows = int(row[2])
        elif '**.columns' in row[1]:
            info_cols = int(row[2])
        elif '**.racks' in row[1]:
            info_racks = int(row[2])
        elif '**.hosts' in row[1]:
            info_hosts = int(row[2])

    # ----- Create and save plots -----
    # ---------------------------------

    # Intra-Cell vs Extra-Cell
    fig, ax = plt.subplots()
    ax.pie([intra_cell, extra_cell], labels=['Intra-Cellular', 'Extra-Cellular'], autopct='%1.1f%%')
    ax.set_title('Traffic Distribution')
    plt.savefig('intravsextra_cell.png')
    plt.clf()

    # Intra-Rack vs Extra-Rack
    fig, ax = plt.subplots()
    ax.pie([intra_rack, extra_rack], labels=['Intra-Rack', 'Extra-Rack'], autopct='%1.1f%%')
    ax.set_title('Traffic Distribution')
    plt.savefig('intravsextra_rack.png')
    plt.clf()

    # Flow Size CDF
    count, bins_count = np.histogram(sizes_list, bins=10)
    pdf = count / sum(count)
    cdf = np.cumsum(pdf)

    plt.plot(bins_count[1:], cdf, label='Flow Size CDF')
    plt.title('Flow Size CDF')
    plt.xlabel('Flow Size (in MiB)')
    plt.ylabel('CDF')
    plt.savefig('flow_size_cdf.png')
    plt.clf()

    # Flow Length CDF
    count, bins_count = np.histogram(lengths_list, bins=10)
    pdf = count / sum(count)
    cdf = np.cumsum(pdf)

    plt.plot(bins_count[1:], cdf, label='Flow Length CDF')
    plt.title('Flow Length CDF')
    plt.xlabel('Flow Length (in sec)')
    plt.ylabel('CDF')
    plt.savefig('flow_length_cdf.png')
    plt.clf()

    # Flow Rate CDF
    count, bins_count = np.histogram(rates_list, bins=10)
    pdf = count / sum(count)
    cdf = np.cumsum(pdf)

    plt.plot(bins_count[1:], cdf, label='Flow Rate CDF')
    plt.title('Flow Rate CDF')
    plt.xlabel('Flow Rate (in MBps)')
    plt.ylabel('CDF')
    plt.savefig('flow_rate_cdf.png')
    plt.clf()

    # Full Traffic Size Heatmap
    # TODO: Currently num cells and num racks is hardcoded. Not ideal.
    num_cells = 9
    num_racks = 8
    heats = []
    for i in range(num_cells):
        for j in range(num_racks):
            rack_heats = []
            for k in range(num_cells):
                for l in range(num_racks):
                    if (i, j, k, l) in heat_dict:
                        rack_heats.append(heat_dict[(i, j, k, l)][0])
                    else:
                        rack_heats.append(0)
            heats.append(rack_heats)

    # TODO: Currently axis labels are also hardcoded.
    axis_labels = [0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7,
                   0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7,
                   0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7, 0, 1, 2, 3, 4, 5, 6, 7]

    sns.set(font_scale=0.5)
    heats = list(map(list, zip(*heats)))
    ax = sns.heatmap(heats, xticklabels=axis_labels, yticklabels=axis_labels, cmap='hot_r')
    ax.set_xlabel('Rack From', fontsize=18)
    ax.yaxis.set_label_text('Rack To', fontsize=18)
    ax.invert_yaxis()

    plt.savefig('traffic_between_racks.png')

    # Add lines to separate cells for easier visual parsing.
    ax.hlines(list(np.arange(0, 80, 8)), *ax.get_xlim(), linewidth=0.5)
    ax.vlines(list(np.arange(0, 80, 8)), *ax.get_ylim(), linewidth=0.5)

    plt.savefig('traffic_between_racks_lines.png')

    plt.clf()

    # Create cursor for querying runAttr table,
    #   this contains the network information we will use.
    # --------------------------------------
    # | runId   | attrName  | attrValue    |
    # --------------------------------------
    cur = con.execute('SELECT * FROM runAttr')

    info_configname = ''
    info_datetime = ''
    info_network = ''
    info_cells = info_rows * info_cols
    for row in cur:
        if 'configname' in row[1]:
            info_configname = row[2]
        elif 'datetime' in row[1]:
            info_datetime = row[2]
        elif 'network' in row[1]:
            info_network = row[2]

    # Generate a table/chart with some generic info about
    #   the simulation.
    fig, ax = plt.subplots(1, 1)
    data = [[info_configname, info_datetime, info_network, info_cells, info_racks, info_cells*info_racks, info_hosts, info_hosts*(info_cells*info_racks)]]
    column_labels = ['Config Name', 'Date-time', 'Network', 'Cells', 'Racks Per Cell', 'Total Racks', 'Hosts Per Rack', 'Total Hosts']
    #ax.axis('tight')
    ax.axis('off')
    table = ax.table(cellText=data, colLabels=column_labels, loc='center')
    table.scale(3, 3)
    table.set_fontsize(24)
    plt.savefig('network_info_table.png',bbox_inches='tight')
    plt.clf()

    # Generate a table/chart with some generic info about
    #   traffic in the simulation.
    fig, ax = plt.subplots(1, 1)
    total_traffic = sum(sizes_list)
    data = [[total_traffic, intra_cell/total_traffic*100, extra_cell/total_traffic*100, intra_rack/total_traffic*100, extra_rack/total_traffic*100]]
    column_labels = ['Total Traffic (in MiB)', 'Intra-Cell %', 'Extra-Cell %', 'Intra-Rack %', 'Extra-Rack %']
    ax.axis('tight')
    ax.axis('off')
    table = ax.table(cellText=data, colLabels=column_labels, loc='center')
    table.scale(3, 3)
    table.set_fontsize(24)
    plt.savefig('network_traffic_info_table.png',bbox_inches='tight')
    plt.clf()

    # TODO: Packet Size CDF having some issues due to the size of the vector table.
    # Packet Size CDF
    # cur = con.execute('SELECT * FROM vector')
    # packet_transfer_ids = []
    # for row in cur:
    #     if 'txPk:vector(packetBytes)' in row[3]:
    #         packet_transfer_ids.append(row[0])
    #
    # cur = con.execute('SELECT * FROM vectorData')
    # packet_sizes = []
    # for id in packet_transfer_ids:
    #     query = 'SELECT * FROM vectorData WHERE vectorId=' + str(id)
    #     cur = con.execute(query)
    #     for row in cur:
    #         packet_sizes.append(row[3])
    #
    # count, bins_count = np.histogram(packet_sizes, bins=10)
    # pdf = count / sum(count)
    # cdf = np.cumsum(pdf)
    #
    # plt.plot(bins_count[1:], cdf, label='Packet Size CDF')
    # plt.title('Packet Size CDF')
    # plt.xlabel('Packet Size (in bytes)')
    # plt.ylabel('CDF')
    # plt.savefig('packet_size_cdf.png')
    # plt.clf()

    # Open a connection to sca database.
    con = create_connection(sca_database)

    # TODO: UPDATE THIS TO USE SQL QUERY INSTEAD OF ITERATING
    # Create cursor for querying scalar table,
    #   this contains the utilization and packet
    #   drop information we will use.
    # -----------------------------------------------------------------------
    # | scalarId   | runID  | moduleName    | scalarName    | scalarValue   |
    # -----------------------------------------------------------------------
    cur = con.execute('SELECT * FROM scalar')

    # Initialize variables to hold the utilization and packet drop information.
    utilizations = []
    transfer_count = 0
    receive_count = 0
    pd_bad_checksum = 0
    pd_wrong_port = 0
    pd_address_resolution_failed = 0
    pd_forwarding_disabled = 0
    pd_hop_limit_reached = 0
    pd_incorrectly_received = 0
    pd_interface_down = 0
    pd_no_interface_found = 0
    pd_no_route_found = 0
    pd_not_addressed_to_us = 0
    pd_queue_overflow = 0
    pd_undefined = 0
    for row in cur:
        if 'rx channel utilization' in row[3]:
            utilizations.append(row[4])
        elif 'txPk:count' in row[3]:
            transfer_count += row[4]
        elif 'rxPkOk:count' in row[3]:
            receive_count += row[4]
        elif 'droppedPkBadChecksum:count' in row[3]:
            pd_bad_checksum += row[4]
        elif 'droppedPkWrongPort:count' in row[3]:
            pd_wrong_port += row[4]
        elif 'packetDropAddressResolutionFailed:count' in row[3]:
            pd_address_resolution_failed += row[4]
        elif 'packetDropForwardingDisabled:count' in row[3]:
            pd_forwarding_disabled += row[4]
        elif 'packetDropHopLimitReached:count' in row[3]:
            pd_hop_limit_reached += row[4]
        elif 'packetDropIncorrectlyReceived:count' in row[3]:
            pd_incorrectly_received += row[4]
        elif 'packetDropInterfaceDown:count' in row[3]:
            pd_interface_down += row[4]
        elif 'packetDropNoInterfaceFound:count' in row[3]:
            pd_no_interface_found += row[4]
        elif 'packetDropNoRouteFound:count' in row[3]:
            pd_no_route_found += row[4]
        elif 'packetDropNotAddressedToUs:count' in row[3]:
            pd_not_addressed_to_us += row[4]
        elif 'packetDropQueueOverflow:count' in row[3]:
            pd_queue_overflow += row[4]
        elif 'packetDropUndefined:count' in row[3]:
            pd_undefined += row[4]

    # Generate a table/chart with some info about
    #   utilization and loss in the simulation.
    fig, ax = plt.subplots()
    avg_utilization = statistics.fmean(utilizations)
    data = [[avg_utilization, int(transfer_count), int(receive_count)]]
    column_labels = ['Average Channel Utilization (%)', 'Packets Transferred', 'Packets Received']
    ax.axis('tight')
    ax.axis('off')
    table = ax.table(cellText=data, colLabels=column_labels, loc='center')
    table.scale(3, 3)
    table.set_fontsize(24)
    plt.savefig('utilization_table.png', bbox_inches='tight')

    data2 = [[pd_bad_checksum, pd_wrong_port, pd_address_resolution_failed, pd_forwarding_disabled,
              pd_hop_limit_reached, pd_incorrectly_received, pd_interface_down, pd_no_route_found,
              pd_not_addressed_to_us, pd_queue_overflow, pd_undefined]]
    column_labels2 = ['Bad Checksum', 'Wrong Port', 'Address Resolution Failed', 'Forwarding Disabled',
                      'Hop Limit Reached', 'Incorrectly Received', 'Interface Down', 'No Route Found',
                      'Not Addressed to Us', 'Queue Overflow', 'Undefined']
    table2 = ax.table(cellText=data2, colLabels=column_labels2, loc='center')
    table2.scale(3,3)
    table2.set_fontsize(24)
    plt.savefig('packet_drop_table.png', bbox_inches='tight')
    plt.clf()


if __name__ == '__main__':
    main()
