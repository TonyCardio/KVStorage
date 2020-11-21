#!/usr/bin/env python3
import sys

if sys.version_info < (3, 6):
    print('Use python >= 3.6', file=sys.stderr)
    sys.exit(1)

import os
import argparse
import textwrap
import json

try:
    from storage.storageclient import StorageClient
except Exception as e:
    print(f"storage module is not found {str(e)}")
    sys.exit(1)


def parse_argument():
    """Parsing arguments"""
    parser = argparse.ArgumentParser(
        prog='client.py',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent('''\
        Client part of KV storage
        --------------------------------
        commands while running:
            auth                                              authorize client
            set {[-r,--raw],[-f,--file]} db_name key={json_value}  send request to set value by key
            get {[-r,--raw],[-f,--file]} db_name key1&key2    send request get value by key
            exit                                              exit client
        '''))
    return parser.parse_args()


def main():
    """Enter point of program"""
    parser = parse_argument()
    conf_path = StorageClient.config_path
    if not os.path.exists(StorageClient.config_path):
        print(f"settings {conf_path} not found")
        sys.exit(1)
    with open(conf_path, "r") as f:
        conf = json.loads(f.read())

    client = StorageClient(
        conf["cluster_node_host"],
        conf["cluster_node_port"],
        with_checker=conf["with_checker"],
        debug=conf["debug"])
    client.run()


if __name__ == '__main__':
    main()
