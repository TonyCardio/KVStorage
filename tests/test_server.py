import os
import sys
import unittest
import shutil

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             os.path.pardir))

from storage.servernode import app
from storage.servernode import memory


class TestServer(unittest.TestCase):
    token = "my_api_key"
    headers = {"Authorization": token}

    @classmethod
    def setUpClass(cls) -> None:
        base_data = {
            "my_database": {"hello": {"key": "hello", "value": "world"}}}
        memory.self_url = "http://localhost:3333"
        app.test_client.post('/registerkey', json={"token": TestServer.token})
        memory.storage[TestServer.token] = base_data

    def test_auth_register_client(self):
        _, response = app.test_client.post('/auth')
        self.assertTrue(response.json["api-key"] in memory.api_keys)

    def test_get_returns_200(self):
        data = {"db_name": "my_database", "keys": ["hello"]}
        _, response = app.test_client.post('/get',
                                           json=data,
                                           headers=TestServer.headers)
        assert response.status == 200

    def test_get_returns_correct_value(self):
        data = {"db_name": "my_database", "keys": ["hello"]}
        _, response = app.test_client.post('/get',
                                           json=data,
                                           headers=TestServer.headers)
        response_data = response.json["entries"]["hello"]
        assert response_data["key"] == "hello"
        assert response_data["value"] == "world"
        assert response.status == 200

    def test_get_returns_500_when_incorrect_request(self):
        incorrect_data = {"incorrect_data": "hello"}
        _, response = app.test_client.post('/get?',
                                           json=incorrect_data,
                                           headers=TestServer.headers)
        assert response.status == 500

    def test_get_returns_404_when_no_key(self):
        data = {"db_name": "my_database", "keys": ["unknown_key"]}
        _, response = app.test_client.post('/get',
                                           json=data,
                                           headers=TestServer.headers)
        assert response.status == 404

    def test_get_after_post_returns_correct_value(self):
        set_data = {"db_name": "my_database",
                    "keys": [{"key": "Sanic", "value": "go fast"}]}
        get_data = {"db_name": "my_database", "keys": ["Sanic"]}

        app.test_client.post('/set',
                             json=set_data,
                             headers=TestServer.headers)
        _, response = app.test_client.post('/get',
                                           json=get_data,
                                           headers=TestServer.headers)

        response_data = response.json["entries"]["Sanic"]
        assert response_data["key"] == "Sanic"
        assert response_data["value"] == "go fast"
        assert response.status == 200

    def test_get_from_file(self):
        data = {"db_name": "my_database",
                "keys": [{"key": "no_in_RAM", "value": "value is on disk"}]}
        app.test_client.post('/set', json=data, headers=TestServer.headers)
        memory.storage[TestServer.token]["my_database"].pop("no_in_RAM")

        get_data = {"db_name": "my_database", "keys": ["no_in_RAM"]}
        _, response = app.test_client.post('/get',
                                           json=get_data,
                                           headers=TestServer.headers)

        response_data = response.json["entries"]["no_in_RAM"]
        assert response_data["key"] == "no_in_RAM"
        assert response_data["value"] == "value is on disk"
        assert response.status == 200

    def test_set_returns_500_when_incorrect_request(self):
        data = {"db_name": "my_database", "keys": [{"value": "some_value"}]}
        _, response = app.test_client.post('/set',
                                           json=data,
                                           headers=TestServer.headers)
        assert response.status == 500

    def test_clusterinfo_returns_correct_urls(self):
        _, response = app.test_client.get('/clusterinfo')
        self.assertTrue("http://localhost:3333" in response.json["addresses"])

    def test_distribute_delete_unreachable_nodes(self):
        data = {"db_name": "my_database", "keys": ["no_in_node"]}
        nodes = memory.cluster_nodes
        memory.cluster_nodes.add("http://127.0.0.3:3333")
        self.assertEqual(len(nodes), 1)
        app.test_client.post('/get',
                             json=data,
                             headers=TestServer.headers)
        # server tries to do quorum
        self.assertEqual(len(nodes), 0)

    def test_connect_cluster_returns_200(self):
        data = {"sender_address": "http://127.0.0.3:3333"}
        _, response = app.test_client.post('/mkcluster', json=data)
        assert response.status == 200

    def test_connect_cluster_returns_500_when_incorrect_request(self):
        data = {"sender_addr": "http://127.0.0.3:3333"}
        _, response = app.test_client.post('/mkcluster', json=data)
        assert response.status == 500

    def test_connect_cluster_does_not_returns_sender_address(self):
        memory.cluster_nodes.add("http://127.0.0.3:9999")
        data = {"sender_address": "http://127.0.0.3:9999"}
        _, response = app.test_client.post('/mkcluster', json=data)
        self.assertTrue(
            "http://127.0.0.3:9999" not in response.json["addresses"])

    def test_register_node_returns_200(self):
        data = {"address": ["http://127.0.0.3:5555"]}
        _, response = app.test_client.post('/registernode', json=data)
        self.assertTrue("http://127.0.0.3:5555" in memory.cluster_nodes)
        assert response.status == 200

    def test_register_key_returns_200(self):
        data = {"token": "some_token"}
        _, response = app.test_client.post('/registerkey', json=data)
        self.assertTrue("some_token" in memory.api_keys)
        assert response.status == 200

    def test_quorum_get_returns_200_when_key_in_node(self):
        data = {"db_name": "my_database",
                "keys": ["hello"],
                "token": TestServer.token,
                "without_key": ["some_nodes_without_key"]}
        _, response = app.test_client.post('/get',
                                           json=data,
                                           headers=TestServer.headers)
        assert response.status == 200

    def test_quorum_get_returns_404_when_key_no_in_node(self):
        data = {"db_name": "my_database",
                "keys": ["no_in_node"],
                "token": TestServer.token,
                "without_key": ["some_nodes_without_key"]}
        _, response = app.test_client.post('/get',
                                           json=data,
                                           headers=TestServer.headers)
        assert response.status == 404

    @classmethod
    def tearDownClass(cls) -> None:
        # if os.path.exists("./data"):
        #     shutil.rmtree("./data")
        pass


if __name__ == '__main__':
    unittest.main()
