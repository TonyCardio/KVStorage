import os
import sys
import unittest
import threading
import shutil
import time
from multiprocessing import Process

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             os.path.pardir))

from storage.storageclient import StorageClient
from storage.servernode import Node, app

node = Node()


def start_serv():
    node.run("127.0.0.1", 9090)


class TestClient(unittest.TestCase):
    serv_host = "127.0.0.1"
    serv_port = 9090
    client = StorageClient(serv_host, serv_port, debug=False,
                           blocking=False, with_checker=False)

    @classmethod
    def setUpClass(cls) -> None:
        cls.server = Process(target=start_serv)
        cls.server.start()
        cls.client_thread = threading.Thread(target=cls.client.run)
        cls.client_thread.start()
        time.sleep(3)

    def test_client_connected_cluster(self):
        urls = TestClient.client.cluster_nodes
        self.assertEqual(len(urls), 1)
        self.assertEqual(urls, {"http://127.0.0.1:9090"})

    def test_do_cluster_info_returns_none_when_no_nodes_available(self):
        urls = TestClient.client.cluster_nodes.copy()
        TestClient.client.cluster_nodes.clear()
        returns = TestClient.client.do_cluster_info()
        TestClient.client.cluster_nodes = urls
        self.assertIsNone(returns)

    def test_do_set_returns_200(self):
        raw = '-r my_database personage={"name":"Homer","surname":"Simpson"}'
        response = TestClient.client.do_set(raw)
        self.assertEqual(response.status_code, 200)

    def test_do_set_returns_401_when_no_auth(self):
        original_key = TestClient.client.api_key
        TestClient.client.api_key = original_key + "salt"
        raw = '-r my_database personage={"name":"Homer","surname":"Simpson"}'
        response = TestClient.client.do_set(raw)
        TestClient.client.api_key = original_key
        self.assertEqual(response.status_code, 401)

    def test_do_set_returns_false_when_incorrect_args(self):
        incorrect_raw = '-r personage={"name":"Homer","surname":"Simpson"}'
        self.assertRaises(ValueError, TestClient.client.do_set, incorrect_raw)

    def test_do_set_returns_none_when_no_nodes_available(self):
        raw = '-r my_database personage={"name":"Homer","surname":"Simpson"}'
        urls = TestClient.client.cluster_nodes.copy()
        TestClient.client.cluster_nodes.clear()
        returns = TestClient.client.do_set(raw)
        TestClient.client.cluster_nodes = urls
        self.assertIsNone(returns)

    def test_do_get_returns_200(self):
        father = '-r simpsons_db father={"name":"Homer","surname":"Simpson"}'
        son = '-r simpsons_db son={"name":"Bart","surname":"Simpson"}'
        TestClient.client.do_set(father)
        TestClient.client.do_set(son)
        response = TestClient.client.do_get("-r simpsons_db father&son")
        self.assertEqual(response.status_code, 200)

    def test_do_get_returns_none_when_no_nodes_available(self):
        raw = '-r my_database personage={"name":"Homer","surname":"Simpson"}'
        urls = TestClient.client.cluster_nodes.copy()
        TestClient.client.cluster_nodes.clear()
        returns = TestClient.client.do_get(raw)
        TestClient.client.cluster_nodes = urls
        self.assertIsNone(returns)

    def test_handle_command_returns_true_when_correct_command(self):
        set_raw = '-r my_database personage={"name":"Homer","surname":"Simpson"}'
        get_raw = '-r my_database personage'
        self.assertTrue(TestClient.client.handle_command(f"set {set_raw}"))
        self.assertTrue(TestClient.client.handle_command(f"get {get_raw}"))

    def test_handle_command_returns_false_when_incorrect_command(self):
        self.assertFalse(TestClient.client.handle_command("unknown_command"))

    @classmethod
    def tearDownClass(cls) -> None:
        app.stop()
        cls.client.exit()
        cls.server.terminate()
        cls.client_thread.join()

        # if os.path.exists("./data"):
        #     shutil.rmtree("./data")
        # if os.path.exists("./api-key.txt"):
        #     os.remove("./api-key.txt")


if __name__ == '__main__':
    unittest.main()
