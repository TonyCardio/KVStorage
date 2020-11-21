import os
import sys
import unittest
import aiounittest

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             os.path.pardir))

from storage.servernode import Node


class TestNode(aiounittest.AsyncTestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.without_seed = Node()
        cls.unreachable_seed = Node("127.0.0.1", 9090)

    async def test_async_connect_cluster_returns_false_when_seed_500(self):
        self.assertFalse(await TestNode.unreachable_seed.connect_cluster())

    async def test_async_connect_cluster_returns_none_when_no_seed(self):
        self.assertIsNone(await TestNode.without_seed.connect_cluster())


if __name__ == '__main__':
    unittest.main()
