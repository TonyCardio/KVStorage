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
    from storage.servernode import Node
    from storage.servernode import app
except Exception as e:
    print(f"storage module is not found {str(e)}")
    sys.exit(1)


def parse_argument():
    """Parsing arguments"""
    parser = argparse.ArgumentParser(
        prog='server.py',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent('''\
        Server part of KV storage
        --------------------------------
        commands while running:
          mkcluster           connect node to cluster
        '''))
    return parser.parse_args()


def main():
    """Enter point of program"""
    parser = parse_argument()
    config_name = "server_conf.json"
    if not os.path.exists(config_name):
        print(f"settings not found")
        sys.exit(1)
    with open(config_name, "r") as f:
        config_name = json.loads(f.read())

    node = Node(seed_host=config_name["seed_host"],
                seed_port=config_name["seed_port"],
                debug=config_name["debug"])

    node.run(host=config_name["server_host"],
             port=config_name["server_port"],
             debug=config_name["debug"],
             access_log=config_name["access_log"])


if __name__ == '__main__':
    main()
