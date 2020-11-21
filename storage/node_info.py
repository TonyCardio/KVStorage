#!/usr/bin/env python3
import os
import json
import aiofiles


# Storage schema
# {"token":
#   {"db_name":
#       {"key1": {"key": "key1", "value": "value1"}}
#   }
# }

class NodeInfo:
    self_url = None
    storage = {}
    cluster_nodes = set()
    api_keys = set()

    @classmethod
    def __init__(cls):
        if not os.path.exists("./data"):
            os.mkdir("./data")
        if not os.path.exists("./data/api_keys.json"):
            with open(f'./data/api_keys.json', 'w') as f:
                f.write(json.dumps({"api_keys": []}))
        else:
            with open(f'./data/api_keys.json', 'r') as f:
                cls.api_keys = set(json.load(f)["api_keys"])

    @classmethod
    def get_cluster_nodes(cls):
        return cls.cluster_nodes

    @classmethod
    def add_cluster_urls(cls, urls: list):
        cls.cluster_nodes.update(urls)

    @classmethod
    def init_new_keys(cls, token, db_name):
        if token not in cls.storage:
            cls.storage[token] = {db_name: {}}
        if db_name not in cls.storage[token]:
            cls.storage[token][db_name] = {}

    @classmethod
    async def add_client_api_key(cls, token):
        """Updating cls.api_keys and api-keys file with new token"""
        cls.api_keys.add(token)
        data = ""
        async with aiofiles.open(f'./data/api_keys.json', 'r') as f:
            data = json.loads(await f.read())
        data["api_keys"].append(token)
        async with aiofiles.open(f'./data/api_keys.json', 'w') as f:
            await f.write(json.dumps(data))

    @classmethod
    async def is_valid_token(cls, token):
        return token in cls.api_keys

    @classmethod
    async def add_keys(cls, token: str = None,
                       db_name: str = None,
                       keys: list = None,
                       **kwargs):
        """
        Add key:value to memory and on disk
        :param token: user`s token
        :param db_name: name of user`s database
        :param keys: list of keys:value pairs to add
        :return:
        """

        if not os.path.exists(f'./data/{token}'):
            os.mkdir(f'./data/{token}')
        if not os.path.exists(f'./data/{token}/{db_name}'):
            os.mkdir(f'./data/{token}/{db_name}')
        cls.init_new_keys(token, db_name)

        for key_data in keys:
            key = key_data["key"]
            cls.storage[token][db_name][key] = key_data
            async with aiofiles.open(
                    f'./data/{token}/{db_name}/{key}.json', 'w') as f:
                await f.write(json.dumps(key_data))

    @classmethod
    async def add_keys_from_other_node(cls, token, db_name, entries, **kwargs):
        keys = [key_value[1] for key_value in entries.items()]
        await cls.add_keys(token, db_name, keys)

    @classmethod
    def get_values_from_memory(cls, token: str, db_name: str, keys: list):
        """
        Get values by keys from RAM storage
        :param token: user`s token
        :param db_name: name of user`s database
        :param keys: list of keys to get
        :return: dict of founded values and not_found_keys
        """
        founded = {}

        if token in cls.storage and db_name in cls.storage[token]:
            for key in keys:
                if key in cls.storage[token][db_name]:
                    founded[key] = cls.storage[token][db_name][key]

        not_found = set(keys) - founded.keys()
        return {"entries": founded, "not_found_keys": list(not_found)}

    @classmethod
    async def get_values_from_disk(cls, token: str, db_name: str, keys: list):
        """
        Get values by keys from disk storage
        :param token: user`s token
        :param db_name: name of user`s database
        :param keys: list of keys to get
        :return: dict of founded values and not_found_keys
        """
        founded = {}

        if os.path.exists(f'./data/{token}') and \
                os.path.exists(f'./data/{token}/{db_name}'):
            for key in keys:
                if os.path.exists(f"./data/{token}/{db_name}/{key}.json"):
                    cls.init_new_keys(token, db_name)
                    async with aiofiles.open(
                            f'./data/{token}/{db_name}/{key}.json', 'r') as f:
                        data = json.loads(await f.read())
                        cls.storage[token][db_name][key] = data
                        founded[key] = data

        not_found = set(keys) - founded.keys()
        return {"entries": founded, "not_found_keys": list(not_found)}

    @classmethod
    async def get_values(cls,
                         token: str = None,
                         db_name: str = None,
                         keys: list = None,
                         **kwargs):
        """
        Get values by keys from storage
        :param token: user`s token
        :param db_name: name of user`s database
        :param keys: list of keys to get
        :return: dict of founded values and not_found_keys
        """

        memory_result = cls.get_values_from_memory(token, db_name, keys)
        if not len(memory_result["not_found_keys"]):
            return memory_result

        not_found_keys = memory_result["not_found_keys"]

        disk_result = await cls.get_values_from_disk(
            token, db_name, not_found_keys)
        disk_result["entries"].update(memory_result["entries"])

        return disk_result
