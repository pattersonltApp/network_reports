"""
spineleaf_network_report.py

This file will generate graphics for a given
network configuration using OMNeT++ SQLite results files.

TODO: Fix throughput calculations.

Useful Links:
https://docs.omnetpp.org/tutorials/pandas/
https://docs.omnetpp.org/tutorials/tictoc/part6/
"""
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import seaborn as sns
import pandas as pd
import sqlite3
from sqlite3 import Error
import statistics
import re


def create_connection(db):
    """
    create_connection is utilized to connect to the OMNet++ results
    file stored as a SQLite file.
    """
    con = None
    try:
        con = sqlite3.connect(db)
    except Error as e:
        print(e)

    return con


def throughput_graph(vec_connection):
    """
    throughput_graph is used to calculate throughput given a SQLite connections

    TODO:   Believe this to currently be not functioning as intended. No visualization attached to calculations yet.
            Could be an issue with logic or which values are being pulled from database.

    Comparison
    https://drive.google.com/file/d/1QTEOLz2_hPtiC5fcV56S--QzgPl3q9l_/view
    A Comparative Study of Data Center Network Architectures.pdf
    """
    # Query the database for the required information.
    # Total packet delay in seconds
    total_delay = vec_connection.execute("""\
                                 SELECT SUM(vectorSum) FROM vector
                                 WHERE  vectorName='endToEndDelay:vector'
                                        and LIKE('%]', moduleName)=1""").fetchall()[0][0]

    # Total count of packets
    total_packet_count_pr = vec_connection.execute("""\
                                 SELECT SUM(vectorCount) FROM vector
                                 WHERE  vectorName='packetReceived:vector(packetBytes)'
                                        and LIKE('%]', moduleName)=1""").fetchall()[0][0]

    # Total Size of Packets in bytes
    total_packet_size = vec_connection.execute("""\
                                 SELECT SUM(vectorSum) FROM vector
                                 WHERE  vectorName='packetReceived:vector(packetBytes)'
                                        and LIKE('%]', moduleName)=1""").fetchall()[0][0]

    # Print information for debugging.
    print('Total Delay: ' + str(total_delay))
    print('Total Packet Count (pr): ' + str(total_packet_count_pr))
    print('Total Packet Size (bytes): ' + str(total_packet_size))
    # Calculate throughput and convert to Mbps.
    average_through_bytes = (sum(range(total_packet_count_pr + 1)) * (total_packet_size / total_packet_count_pr)) / total_delay
    average_through_bits = average_through_bytes * 8
    average_through_megabits = average_through_bits / 10 ** 6
    print('Average Throughput (megabits per second): ' + str(average_through_megabits))
    print('Average Packet Delay: ' + str(total_delay / total_packet_count_pr))
    print('Average Packet Size: ' + str(total_packet_size / total_packet_count_pr))


def attribute_table(sca_connection):
    """
    attribute_table is used to create a table of attributes describing
    basic information about the table.
    """
    # Query the database for the required information.
    info_configname = sca_connection.execute("""\
                                SELECT attrValue FROM runAttr
                                WHERE  attrName='configname'""").fetchall()[0][0]

    info_datetime = sca_connection.execute("""\
                                SELECT attrValue FROM runAttr
                                WHERE  attrName='datetime'""").fetchall()[0][0]

    info_experiment = sca_connection.execute("""\
                                SELECT attrValue FROM runAttr
                                WHERE  attrName='experiment'""").fetchall()[0][0]

    info_network = sca_connection.execute("""\
                                SELECT attrValue FROM runAttr
                                WHERE  attrName='network'""").fetchall()[0][0]

    info_total_apps = sca_connection.execute("""\
                                 SELECT SUM(paramValue) FROM runParam
                                 WHERE  LIKE('%.numApps', paramKey)=1""").fetchall()[0][0]

    info_leafs = sca_connection.execute("""\
                                SELECT paramValue FROM runParam
                                WHERE  LIKE('%.leafs', paramKey)=1""").fetchall()[0][0]

    info_hosts = sca_connection.execute("""\
                                SELECT paramValue FROM runParam
                                WHERE  LIKE('%.hosts', paramKey)=1""").fetchall()[0][0]

    # TODO: Number of spines currently must be hardcoded, not ideal
    info_spines = 3

    # Generate a table and insert the information.
    fig, ax = plt.subplots(1, 1)
    data = [[info_configname, info_datetime, info_network, info_experiment,
             info_spines, info_leafs, info_hosts, info_total_apps]]
    column_labels = ['Config Name', 'Date-time', 'Network', 'Experiment', 'Spines', 'Leaves', 'Hosts', 'Total Apps']
    ax.axis('off')
    table = ax.table(cellText=data, colLabels=column_labels, loc='center')
    table.scale(3, 3)
    table.set_fontsize(24)
    plt.savefig('spineleaf_dc_info_table.png', bbox_inches='tight')
    plt.clf()


def traffic_graphics(sca_connection):
    """
    traffic_graphics is intended to create graphics describing the traffic
    within the network simulation.

    The timing and size of 'TcpSessionApp' is used to calculate the flow and track
    traffic. This may be an incorrect representation, more research is needed on how to
    calculate flow in the network.

    Currently this function uses a cursor to iterate over the runParam table and stores
    the information describing the connections and calculates the total time (tOpen + tSend + tClose)
    taken and size (sendBytes).
    TODO: This can probably be accomplished with querying somehow instead of iterating.
    """
    # Create cursor for querying runParam table,
    #   this contains the information we will use.
    # -------------------------------------------------------
    # | runId   | paramKey  | paramValue    | paramOrder    |
    # -------------------------------------------------------
    cur = sca_connection.execute('SELECT * FROM runParam')

    # head_dict will hold a dictionary of the total amount of data sent during connections
    # [(frm_leaf, frm_host, to_leaf, to_host) : (size, length)]
    heat_dict = {}

    sizes_list = []
    lengths_list = []
    rates_list = []
    length = 0
    size = 0
    intra_leaf = 0
    extra_leaf = 0

    # Regex patterns for extracting spine, leaf, host
    pattern1 = '.*\[(\d*)\]\..*\[(\d*)\]..*\[(\d*)\].*'
    pattern2 = '.*\[(\d*)\]\..*\[(\d*)\].*'

    # Iterate over database
    for row in cur:
        # If row is sendBytes we store the size being sent
        if 'sendBytes' in row[1]:
            size = int(row[2][:-3])
            sizes_list.append(size)
        # If row is tOpen we reset the length to whatever is stored in the paramValue
        elif 'tOpen' in row[1]:
            length = int(row[2][:-1])
        # If row is tSend we add the time to our length
        elif 'tSend' in row[1]:
            length += int(row[2][:-1])
        # If row is tClose we add the length and append the total length to our list
        elif 'tClose' in row[1]:
            length += int(row[2][:-1])
            lengths_list.append(length)
            rates_list.append(size / length)
        # connectAddress always comes last after the other information
        elif 'connectAddress' in row[1]:
            # Use regex to extract host and leaf information
            capture = re.search(pattern1, row[1])

            frm_leaf = int(capture.group(1))
            frm_host = int(capture.group(2))

            capture = re.search(pattern2, row[2])

            to_leaf = int(capture.group(1))
            to_host = int(capture.group(2))

            # Create entry or update heat dictionary
            if (frm_leaf, frm_host, to_leaf, to_host) in heat_dict:
                old_size, old_len = heat_dict[(frm_leaf, frm_host, to_leaf, to_host)]
                heat_dict[(frm_leaf, frm_host, to_leaf, to_host)] = (old_size + size, old_len + length)
            else:
                heat_dict[(frm_leaf, frm_host, to_leaf, to_host)] = (size, length)

            if frm_leaf == to_leaf:
                intra_leaf += size
            else:
                extra_leaf += size

    # Plot the results.
    # Intra-Leaf vs Extra-Leaf
    fig, ax = plt.subplots()
    ax.pie([intra_leaf, extra_leaf], labels=['Intra-Leaf', 'Extra-Leaf'], autopct='%1.1f%%')
    ax.set_title('Leaf Traffic')
    plt.savefig('spineleaf_intravsextra_leaf.png')
    plt.clf()

    # Flow Size CDF scaled like literature
    # 'Network Traffic Characteristics of Data Centers in the Wild'
    sizes_list_bytes = [i * 1048576 for i in sizes_list] # Convert MiB to bytes
    count, bins_count = np.histogram(sizes_list_bytes, bins=50)
    pdf = count / sum(count)
    cdf = np.cumsum(pdf)

    positions = [1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000]
    labels = ['1', '10', '100', '1000', '10000', '100000', '1e+06', '1e+07', '1e+08']

    fig, ax = plt.subplots()
    ax.plot(bins_count[1:], cdf, label='Flow Size CDF', marker='o', markersize=4)
    plt.xscale('log')
    fig = plt.gcf()
    fig.set_figwidth(9)
    fig.set_figheight(4)
    ax.set_xticks(positions)
    ax.set_xticklabels(labels)
    plt.title('Flow Size CDF')
    plt.xlabel('Flow Size (in bytes)')
    plt.ylabel('CDF')
    plt.savefig('flow_size_cdf.png')
    plt.clf()

    # Flow Size CDF
    count, bins_count = np.histogram(sizes_list_bytes, bins=50)
    pdf = count / sum(count)
    cdf = np.cumsum(pdf)

    fig, ax = plt.subplots()
    ax.plot(bins_count[1:], cdf, label='Flow Size CDF', marker='o', markersize=4)
    plt.xscale('log')
    fig = plt.gcf()
    fig.set_figwidth(9)
    fig.set_figheight(4)
    positions = [1e06, 1e07, 1e08]
    ax.set_xticks(positions)
    ax.xaxis.set_major_formatter(mtick.FormatStrFormatter('%.2e'))
    plt.title('Flow Size CDF')
    plt.xlabel('Flow Size (in bytes)')
    plt.ylabel('CDF')
    plt.savefig('flow_size_notscaled_cdf.png')
    plt.clf()

    # Flow Length CDF scaled like literature
    # 'Network Traffic Characteristics of Data Centers in the Wild'
    lengths_list_usecs = [i * 1000000 for i in lengths_list]
    count, bins_count = np.histogram(lengths_list_usecs, bins=10)
    pdf = count / sum(count)
    cdf = np.cumsum(pdf)

    plt.plot(bins_count[1:], cdf, label='Flow Length CDF')

    positions = [1, 10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000]
    labels = ['1', '10', '100', '1000', '10000', '100000', '1e+06', '1e+07', '1e+08', '1e+09']
    plt.xscale('log')
    fig = plt.gcf()
    fig.set_figwidth(9)
    fig.set_figheight(4)
    ax = plt.gca()
    ax.set_xticks(positions)
    ax.set_xticklabels(labels)
    plt.title('Flow Length CDF')
    plt.xlabel('Flow Length (in usecs)')
    plt.ylabel('CDF')
    plt.savefig('flow_length_cdf.png')
    plt.clf()

    # Flow Length CDF
    count, bins_count = np.histogram(lengths_list_usecs, bins=10)
    pdf = count / sum(count)
    cdf = np.cumsum(pdf)

    plt.plot(bins_count[1:], cdf, label='Flow Length CDF')
    plt.xscale('log')
    fig = plt.gcf()
    fig.set_figwidth(9)
    fig.set_figheight(4)
    plt.title('Flow Length CDF')
    plt.xlabel('Flow Length (in usecs)')
    plt.ylabel('CDF')
    plt.savefig('flow_length_not_scaled_cdf.png')
    plt.clf()

    # Flow Rate CDF scaled like literature
    # 'Network Traffic Characteristics of Data Centers in the Wild'
    rates_list_bitspersec = [i * 8 for i in rates_list]
    count, bins_count = np.histogram(rates_list_bitspersec, bins=10)
    pdf = count / sum(count)
    cdf = np.cumsum(pdf)

    plt.plot(bins_count[1:], cdf, label='Flow Rate CDF')
    plt.xscale('log')
    fig = plt.gcf()
    fig.set_figwidth(10)
    fig.set_figheight(4)

    positions = [0.0001, 0.001, 0.01, 0.1, 1, 10, 100, 1000]
    labels = ['0.0001', '0.001', '0.01', '0.1', '1', '10', '100', '1000']
    ax = plt.gca()
    ax.set_xticks(positions)
    ax.set_xticklabels(labels)

    plt.title('Flow Rate CDF')
    plt.xlabel('Flow Rate (in Mbps)')
    plt.ylabel('CDF')
    plt.savefig('flow_rate_cdf.png')
    plt.clf()

    # Flow Rate CDF
    rates_list_bitspersec = [i * 8 for i in rates_list]
    count, bins_count = np.histogram(rates_list_bitspersec, bins=10)
    pdf = count / sum(count)
    cdf = np.cumsum(pdf)

    plt.plot(bins_count[1:], cdf, label='Flow Rate CDF')
    plt.xscale('log')
    fig = plt.gcf()
    fig.set_figwidth(10)
    fig.set_figheight(4)

    plt.title('Flow Rate CDF')
    plt.xlabel('Flow Rate (in Mbps)')
    plt.ylabel('CDF')
    plt.savefig('flow_rate_cdf.png')
    plt.clf()

    # TODO: Traffic Heatmap using Seaborn and heat dict.


def utilization_and_drop_graphics(sca_connection):
    """
    spineleaf_utilization_and_drop_graphics_sql is used to calculate and visualize
    information regarding the utilization of links within the network.

    TODO: Currently iterates over the database but could probably be done by querying the database instead.
    """
    # Create cursor for querying scalar table,
    #   this contains the utilization and packet
    #   drop information we will use.
    # -----------------------------------------------------------------------
    # | scalarId   | runID  | moduleName    | scalarName    | scalarValue   |
    # -----------------------------------------------------------------------
    cur = sca_connection.execute('SELECT * FROM scalar')

    # Initialize values for all the informatino we will be storing.
    # This includes utilization information and information for each type of drop.
    utilizations = []
    spine_utilizations = []
    leaf_utilizations = []
    transfer_count = 0
    receive_count = 0
    tr_spine_count = 0
    tr_leaf_count = 0
    re_spine_count = 0
    re_leaf_count = 0
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
            if 'spine[' in row[2]:
                spine_utilizations.append(row[4])
            else:
                leaf_utilizations.append(row[4])
        elif 'txPk:count' in row[3]:
            transfer_count += int(row[4])
            if 'spine[' in row[2]:
                tr_spine_count += int(row[4])
            elif 'leaf[' or 'borderLeaf' in row[2]:
                tr_leaf_count += int(row[4])
        elif 'rxPkOk:count' in row[3]:
            receive_count += int(row[4])
            if 'spine[' in row[2]:
                re_spine_count += int(row[4])
            elif 'leaf[' or 'borderLeaf' in row[2]:
                re_leaf_count += int(row[4])
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
    avg_leaf_utilization = statistics.fmean(leaf_utilizations)
    avg_spine_utilization = statistics.fmean(spine_utilizations)

    data = [[avg_utilization, avg_spine_utilization, avg_leaf_utilization]]
    column_labels = ['Average Channel Utilization (%)', 'Average Spine Channel Utilization (%)', 'Average Leaf Channel Utilization (%)']
    ax.axis('tight')
    ax.axis('off')
    table = ax.table(cellText=data, colLabels=column_labels, loc='center')
    table.scale(3, 3)
    table.set_fontsize(24)
    plt.savefig('spineleaf_utilization_table.png', bbox_inches='tight')

    data2 = [[pd_bad_checksum, pd_wrong_port, pd_address_resolution_failed, pd_forwarding_disabled,
              pd_hop_limit_reached, pd_incorrectly_received, pd_interface_down, pd_no_route_found,
              pd_not_addressed_to_us, pd_queue_overflow, pd_undefined]]
    column_labels2 = ['Bad Checksum', 'Wrong Port', 'Address Resolution Failed', 'Forwarding Disabled',
                      'Hop Limit Reached', 'Incorrectly Received', 'Interface Down', 'No Route Found',
                      'Not Addressed to Us', 'Queue Overflow', 'Undefined']
    table2 = ax.table(cellText=data2, colLabels=column_labels2, loc='center')
    table2.scale(3, 3)
    table2.set_fontsize(24)
    plt.savefig('spineleaf_packet_drop_table.png', bbox_inches='tight')

    data3 = [[transfer_count, tr_spine_count, tr_leaf_count, receive_count, re_spine_count, re_leaf_count]]
    column_labels3 = ['Packets Transferred', 'Packets Transferred From Spine', 'Packets Transferred from Leaf', 'Packets Received', 'Packets Received in Spine', 'Packets Received in Leaf']
    table3 = ax.table(cellText=data3, colLabels=column_labels3, loc='center')
    table3.scale(3, 3)
    table3.set_fontsize(24)
    plt.savefig('spineleaf_packet_table.png', bbox_inches='tight')
    plt.clf()

    # Utilization CDF
    count, bins_count = np.histogram(utilizations, bins=100)
    pdf = count / sum(count)
    cdf = np.cumsum(pdf)

    positions = [0.01, 0.1, 1, 10, 100]
    labels = ['0.01', '0.1', '1', '10', '100']
    plt.plot(bins_count[1:], cdf, label='Utilization CDF', marker='o', markersize=4)
    plt.xscale('log')
    fig = plt.gcf()
    fig.set_figwidth(9)
    fig.set_figheight(4)
    ax = plt.gca()
    ax.set_xticks(positions)
    ax.set_xticklabels(labels)
    plt.title('Utilization CDF')
    plt.xlabel('Utilization')
    plt.ylabel('CDF')
    plt.savefig('utilization_cdf.png')
    plt.clf()

    # Spine Utilization CDF
    count, bins_count = np.histogram(spine_utilizations, bins=100)
    pdf = count / sum(count)
    cdf = np.cumsum(pdf)

    positions = [0.01, 0.1, 1, 10, 100]
    labels = ['0.01', '0.1', '1', '10', '100']
    plt.plot(bins_count[1:], cdf, label='Spine Utilization CDF', marker='o', markersize=4)
    plt.xscale('log')
    fig = plt.gcf()
    fig.set_figwidth(9)
    fig.set_figheight(4)
    ax = plt.gca()
    ax.set_xticks(positions)
    ax.set_xticklabels(labels)
    plt.title('Spine Utilization CDF')
    plt.xlabel('Spine Utilization')
    plt.ylabel('CDF')
    plt.savefig('spine_utilization_cdf.png')
    plt.clf()

    # Leaf Utilization CDF
    count, bins_count = np.histogram(leaf_utilizations, bins=100)
    pdf = count / sum(count)
    cdf = np.cumsum(pdf)

    positions = [0.01, 0.1, 1, 10, 100]
    labels = ['0.01', '0.1', '1', '10', '100']
    plt.plot(bins_count[1:], cdf, label='Leaf Utilization CDF', marker='o', markersize=4)
    plt.xscale('log')
    fig = plt.gcf()
    fig.set_figwidth(9)
    fig.set_figheight(4)
    ax = plt.gca()
    ax.set_xticks(positions)
    ax.set_xticklabels(labels)
    plt.title('Leaf Utilization CDF')
    plt.xlabel('Leaf Utilization')
    plt.ylabel('CDF')
    plt.savefig('leaf_utilization_cdf.png')
    plt.clf()


def main():
    # Path for database to be opened.
    vec_database = '/workspaces/share/spineleaf/test-#0.vec'
    sca_database = '/workspaces/share/spineleaf/test-#0.sca'

    # Create connections to database files.
    vec_connection = create_connection(vec_database)
    sca_connection = create_connection(sca_database)

    # Create visualizations.
    attribute_table(sca_connection)
    traffic_graphics(sca_connection)
    utilization_and_drop_graphics(sca_connection)
    throughput_graph(vec_connection)

    # Close connections.
    vec_connection.close()
    sca_connection.close()


if __name__ == '__main__':
    main()
