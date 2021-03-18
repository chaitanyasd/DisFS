"""
Client is used to request data/ send data to and from minions.
todo:
1. Add exception handling
2. Add support to know if a minion is unreachable and policy to allocate blocks to other minions
3. Policy to handle corrupt blocks
4. Add encryption
"""

import rpyc
import traceback
import sys
import os

DATA_DIR = "C:\\Users\\chaitanyad\\Documents\\Personal\\DisFS\\received-data-dir\\"


def get(master_connection, minions, file_name):
    if not master_connection.file_exists(file_name):
        print("File not present...")
        return

    file_table = master_connection.read(file_name)
    block_size = master_connection.get_block_size()
    file_path = DATA_DIR + file_name
    full_file_received = True

    if not os.path.isfile(file_path):
        open(file_path, "wb")

    with open(file_path, "wb") as f:
        for block_index in range(len(file_table)):
            block_info = file_table[block_index]
            block_uuid = block_info[0]
            minions_holding_block = block_info[1]
            block_found = False
            f.seek(block_index * block_size)

            for minion in [minions[minion_id] for minion_id in minions_holding_block]:
                host, port = minion
                block_data = read_from_minion(host, port, block_uuid)
                if block_data is not None:
                    f.write(block_data)
                    # sys.stdout.write(block_data)
                    block_found = True
                    break

            if not block_found:
                # print("No block found", block_uuid)
                print("Skipped - ", block_index + 1, " / ", len(file_table))
                full_file_received = False
            else:
                print("Received - ", block_index + 1, " / ", len(file_table))

    if not full_file_received:
        os.remove(file_path)
        print("Error receiving - ", file_name)
    else:
        print("File ", file_name, " received.")


def put(master_connection, file_name, file_identifier):
    file_size = os.path.getsize(file_name)
    blocks_info = master_connection.write(file_identifier, file_size)
    block_size = master_connection.get_block_size()

    with open(file_name, "rb") as f:
        total_blocks = len(blocks_info)
        i = 0
        for block in blocks_info:
            block_data = f.read(block_size)
            block_uuid = block[0]
            minions = [master_connection.get_minions()[_] for _ in block[1]]
            send_to_minion(block_uuid, block_data, minions)
            i += 1
            print("Sending - ", i, " / ", total_blocks)

    print ("File sent!")

def send_to_minion(block_uuid, block_data, minions):
    current_minion = minions[0]
    remaining_minions = minions[1:]

    host, port = current_minion
    current_minion_conn = rpyc.connect(host, port=port)
    current_minion_conn_root = current_minion_conn.root.Minion()
    current_minion_conn_root.put(block_uuid, block_data, remaining_minions)


def read_from_minion(host, port, block_uuid):
    minion_connection = rpyc.connect(host, port)
    minion_connection_root = minion_connection.root.Minion()
    block_data = minion_connection_root.get(block_uuid)
    return block_data


if __name__ == "__main__":
    try:
        # create DATA_DIR
        if not os.path.isdir(DATA_DIR):
            os.mkdir(DATA_DIR)

        args = sys.argv[1:]
        master_connection = rpyc.connect("localhost", port=1111)
        master_connection_root = master_connection.root.Master()
        minions = master_connection_root.get_minions()

        if args[0] == 'GET':
            get(master_connection_root, minions, args[1])
        elif args[0] == 'PUT':
            put(master_connection_root, args[1], args[2])
        else:
            print("Error in the input format. Please try again.")
    except Exception as e:
        traceback.print_exc(e)
