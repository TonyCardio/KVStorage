#!/usr/bin/env python3
from sanic import Sanic
from storage.node_info import NodeInfo
from storage.token_auth import SanicTokenAuth
from sanic.response import json
from aioconsole import ainput
from requests_async import post
from requests_async import ConnectionError
import json as _json
import uuid

app = Sanic(name="node")
memory = NodeInfo()
auth = SanicTokenAuth(token_verifier=memory.is_valid_token)


@app.route("/auth", methods=["POST"])
async def auth_key(request):
    token = str(uuid.uuid4())
    await memory.add_client_api_key(token)
    await distribute({"token": token}, "/registerkey")
    return json({"api-key": token}, status=200)


@app.route("/clusterinfo", methods=["GET"])
async def get_cluster_info(request):
    data = list(memory.get_cluster_nodes())
    data.append(memory.self_url)
    return json({"addresses": data}, status=200)


@app.route("/set", methods=["POST"])
@auth.auth_required
async def set_value(request):
    try:
        json_args = request.json
        json_args["token"] = request.headers["authorization"]

        await memory.add_keys(**json_args)

        if "is_endpoint" not in request.args:
            await distribute(
                json_args,
                "/set?is_endpoint=True",
                headers={"Authorization": json_args["token"]})

        return json(json_args, status=200)
    except Exception as err:
        return json({"message": f"setting value failed: {str(err)}"},
                    status=500)


@app.route("/get", methods=["POST"])
@auth.auth_required
async def get_value(request):
    try:
        json_args = request.json
        json_args["token"] = request.headers["authorization"]

        data = await memory.get_values(**json_args)
        if not len(data["not_found_keys"]):
            return json(data, status=200)

        json_args["keys"] = data["not_found_keys"]
        if "without_key" not in json_args:
            json_args["without_key"] = []
        json_args["without_key"].append(memory.self_url)

        resp = await distribute(json_args, "/get",
                                headers={"Authorization": json_args["token"]},
                                is_quorum_get=True)
        if resp.status == 200:
            data = _json.loads(resp.body)
            await memory.add_keys_from_other_node(**json_args, **data)
        return resp
    except Exception as err:
        return json({"message": f"getting value failed: {err}"},
                    status=500)


async def distribute(data, url, headers=None, is_quorum_get: bool = False):
    """
    Send request to available nodes
    :param data: json data to send
    :param url: request part
    :param headers: http headers
    :param is_quorum_get: flag to wait response after request
    """
    unreachable = set()
    send_to = memory.get_cluster_nodes()
    if is_quorum_get:
        send_to = send_to.difference(set(data["without_key"]))
    response = json(data, status=404)
    for node in send_to:
        try:
            print(f"try send {url} to {node}")
            response = await post(f"{node}{url}", json=data, headers=headers)
            if is_quorum_get:
                response = json(response.json(),
                                status=response.status_code)
                break
        except ConnectionError:
            unreachable.add(node)
    memory.cluster_nodes.difference(unreachable)
    return response


@app.route("/mkcluster", methods=["POST"])
async def connect_cluster(request):
    """
    Send to all nodes information about new node in cluster
    :return: information about cluster nodes
    """
    try:
        sender = request.json["sender_address"]
        await distribute({"address": [sender]}, "/registernode")
        addresses = list(memory.get_cluster_nodes())
        addresses.append(memory.self_url)
        if sender in addresses:
            addresses.remove(sender)
        memory.add_cluster_urls([sender])
        return json({"addresses": addresses,
                     "api-keys": list(memory.api_keys)},
                    status=200)
    except Exception as err:
        return json({"message": f"making cluster failed: {str(err)}"},
                    status=500)


@app.route("/registernode", methods=["POST"])
async def register_node(request):
    """ Add new node address to local list of nodes """
    memory.add_cluster_urls(request.json["address"])
    return json(request.json, status=200)


@app.route("/registerkey", methods=["POST"])
async def register_key(request):
    """Add new api key to local keys storage"""
    await memory.add_client_api_key(request.json["token"])
    return json(request.json, status=200)


class Node:
    """Implements cluster Node"""

    """(commands, number of args) and methods to handle each command"""
    commands = {("mkcluster", 1): lambda self: self.connect_cluster(),
                ("connections", 1): lambda self: self.print_connections()}

    def __init__(self, seed_host: str = None, seed_port: int = None,
                 debug: bool = False):
        self.debug = debug
        self.seed_host = seed_host
        self.seed_port = seed_port
        self.__seed_url = None
        self.seed_url = f"http://{seed_host}:{seed_port}"

    @property
    def seed_url(self):
        return self.__seed_url

    @seed_url.setter
    def seed_url(self, value):
        if self.seed_host and self.seed_port:
            self.__seed_url = value

    def run(self, host: str = None, port: int = None, debug: bool = False,
            access_log: bool = False):
        """Starting Sanic"""
        memory.self_url = f"http://{host}:{port}"
        app.add_task(self.main_loop())
        app.run(host, port, debug=debug, access_log=access_log)

    @staticmethod
    async def print_connections():
        print(memory.cluster_nodes)

    async def main_loop(self):
        """Main loop of the server to handle admin`s commands"""
        try:
            while True:
                inp = await ainput('Insert command >')
                args = str(inp).split(" ")
                if (args[0], len(args)) in Node.commands:
                    command_key = (args[0], len(args))
                    await Node.commands[command_key](self, *args[1:])
                else:
                    self.debug_print("Unknown command")
        except Exception as e:
            self.debug_print(str(e))

    async def connect_cluster(self):
        """Take information about cluster from seed_node"""
        if not self.seed_url:
            self.debug_print("There is no seed server")
            return None
        try:
            response = await post(f"{self.seed_url}/mkcluster",
                                  json={"sender_address": memory.self_url})
            data = response.json()
            memory.add_cluster_urls(data["addresses"])
            memory.api_keys.update(data["api-keys"])
            return True
        except ConnectionError:
            self.debug_print(f"Seed node in unreachable")
            return False

    def debug_print(self, message):
        """Print message if debug"""
        if self.debug:
            print(f"[DEBUG]: {message}")
