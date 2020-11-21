#!/usr/bin/env python3
import sys
import os
from requests import get, post
from requests import ConnectionError
import threading
import time
import json
from json.decoder import JSONDecodeError
from PyQt5.QtCore import QObject, pyqtSignal


class CriticalErrorSignal(QObject):
    enter_message = pyqtSignal(str, name="onError")

    def make_signal(self, message):
        self.onError.emit(message)


class ServiceSignal(QObject):
    enter_message = pyqtSignal(str, name="onService")

    def make_signal(self, message):
        self.onService.emit(message)


class StorageClient:
    """Implements client"""

    """commands and methods to handle each command"""
    commands = {"auth": lambda self, req: self.do_auth(),
                "get": lambda self, req: self.do_get(req),
                "set": lambda self, req: self.do_set(req),
                "exit": lambda self, req: self.exit()}

    config_path = "client_conf.json"
    api_key_path = "api-key.txt"

    def __init__(self,
                 serv_host,
                 serv_port,
                 debug: bool = False,
                 blocking: bool = True,
                 with_checker: bool = True,
                 checker_interval: int = 20):
        self.err_signal = CriticalErrorSignal()
        self.service_signal = ServiceSignal()

        self.cluster_node_address = f"http://{serv_host}:{serv_port}"
        self.cluster_nodes = {self.cluster_node_address}
        self.api_key = self.try_load_api_key()
        self.debug = debug
        self.blocking = blocking
        self.with_checker = with_checker
        self.checker_interval = checker_interval
        self.is_stopping = False

    @staticmethod
    def prepare_set_data_from_raw(db_name, raw):
        keys_values = raw.split("=", maxsplit=1)

        if len(keys_values) != 2:
            raise ValueError("key=value schema expected")

        try:
            return {"db_name": db_name,
                    "keys": [
                        {"key": keys_values[0],
                         "value": json.loads(keys_values[1])}]}
        except JSONDecodeError:
            raise

    @staticmethod
    def prepare_set_data_from_file(db_name, filename):
        if not os.path.exists(filename):
            raise ValueError("file does not exist")

        with open(filename) as f:
            data = json.loads(f.read())

        returns = {"db_name": db_name, "keys": []}
        for key in data.keys():
            returns["keys"].append({"key": key, "value": data[key]})

        return returns

    @staticmethod
    def prepare_get_data_from_raw(db_name, raw):
        if len(raw) == 0:
            raise ValueError("at least one key expected but zero given")

        return {"db_name": db_name, "keys": raw.split("&")}

    @staticmethod
    def prepare_get_data_from_file(db_name, filename):
        if not os.path.exists(filename):
            raise ValueError("file does not exist")

        with open(filename) as f:
            keys = json.loads(f.read())

        return {"db_name": db_name, "keys": keys}

    def try_load_api_key(self):
        api_key_path = StorageClient.api_key_path
        api_key = None
        if os.path.exists(api_key_path):
            with open(api_key_path, "r")as f:
                api_key = f.read()

        if not api_key:
            self.err_signal.make_signal("api-key not found")

        return api_key

    def run(self):
        """Start client"""

        if not self.api_key:
            response_auth = self.do_auth()
            if not response_auth:
                self.err_signal.make_signal(
                    "Server did not respond to request to get api-key")
                sys.exit(1)

        response_cluster_inf = self.do_cluster_info()
        if not response_cluster_inf:
            sys.exit(1)

        if self.with_checker:
            checker = threading.Thread(target=self.run_checker)
            checker.start()

        self.main_loop()

        if self.with_checker:
            checker.join()
        self.d_print("exiting client")

    def main_loop(self):
        while not self.is_stopping:
            try:
                if self.blocking:
                    inp = input('Insert command >')
                    self.handle_command(inp)
            except ValueError as err:
                self.d_print(str(err))
            except Exception as e:
                self.d_print(str(e))
                self.exit()

    def run_checker(self):
        """Starts up loop that upgrades cluster information"""
        while not self.is_stopping:
            time.sleep(self.checker_interval)
            self.do_cluster_info(checker=True)

    def handle_command(self, inp):
        """
        Check command for existence
        If the command exists, calls the appropriate method
        :param inp: potential command
        :return: True if command is correct
        """
        command = inp if inp.find(" ") == -1 \
            else inp[:inp.find(" ")]
        if command in StorageClient.commands:
            args = inp[len(command) + 1:].lstrip()
            try:
                return StorageClient.commands[command](self, args)
            except JSONDecodeError:
                self.d_print("JSONDecodeError")
        else:
            self.d_print("Unknown command")
            return None

    def do_auth(self):
        response = self.send_request(lambda url, headers:
                                     post(f"{url}/auth"), with_auth=False)

        if response is None:
            self.d_print(f"(do_auth) no servers are available")
            return None

        api_key = response.json()["api-key"]
        self.api_key = api_key
        with open(StorageClient.api_key_path, "w") as f:
            f.write(api_key)

        return response

    def do_cluster_info(self, checker=False):
        if not checker:
            self.d_print("(do_cluster_info) sending")
        response = self.send_request(lambda url, headers:
                                     get(f"{url}/clusterinfo",
                                         headers=headers), with_auth=False)
        if response is None:
            if not checker:
                self.d_print(
                    f"(do_cluster_info) no servers are available")
            return None

        urls = response.json()["addresses"]
        self.cluster_nodes.update(urls)
        if not checker:
            self.d_print(
                f"(do_cluster_info) status_code: {response.status_code}")
        return response.status_code

    def do_get(self, args):
        """
        Send GET request
        :param args: string with schema: key1&key2&...&keyN
        :return: response object
        """
        self.d_print("(do_get) sending")

        args = args.split(" ", maxsplit=2)
        if len(args) < 3:
            raise ValueError(
                "(do_get) 3 arguments were expected but less given")

        input_type = args[0]
        json_data = {}
        try:
            if input_type == "-f" or input_type == "--file":
                json_data = StorageClient.prepare_get_data_from_file(*args[1:])
            elif input_type == "-r" or input_type == "--raw":
                json_data = StorageClient.prepare_get_data_from_raw(*args[1:])
        except JSONDecodeError:
            raise
        except ValueError:
            raise

        response = self.send_request(
            lambda url, headers: post(f"{url}/get",
                                      json=json_data,
                                      headers=headers))
        if response is None:
            self.d_print(f"(do_get) no servers are available")
            return None
        self.d_print(
            f"(do_get) response status_code: {response.status_code}")

        print(response.json())
        return response

    def do_set(self, args):
        """
        Send POST request to set pairs key:value to storage
        :param args: string with schema: db_name input_type key=value
        :return: response object
        """
        self.d_print("(do_set) sending")

        args = args.split(" ", maxsplit=2)
        if len(args) < 3:
            raise ValueError("(do_set) 3 arguments were expected but less given")

        input_type = args[0]
        data = {}
        try:
            if input_type == "-f" or input_type == "--file":
                data = StorageClient.prepare_set_data_from_file(*args[1:])
            elif input_type == "-r" or input_type == "--raw":
                data = StorageClient.prepare_set_data_from_raw(*args[1:])
        except JSONDecodeError:
            raise
        except ValueError:
            raise

        response = self.send_request(lambda url, headers:
                                     post(f"{url}/set",
                                          json=data,
                                          headers=headers))
        if response is None:
            self.d_print(f"(do_set) no servers are available")
            return None

        self.d_print(
            f"(do_set) response status_code: {response.status_code}")

        print(response.json())
        return response

    def send_request(self, request, with_auth=True):
        headers = {"Authorization": self.api_key} if with_auth else None
        response = None
        for node_url in self.cluster_nodes:
            try:
                response = request(node_url, headers)
                break
            except ConnectionError:
                pass
        return response

    def exit(self):
        """Stop Client"""
        self.is_stopping = True

    def d_print(self, message: str):
        """Print message if debug"""
        if self.debug:
            message = f"[DEBUG]: {message}"
            self.service_signal.make_signal(message)
            print(message)
