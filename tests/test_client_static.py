import os
import sys
import unittest
import json
from json.decoder import JSONDecodeError

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                             os.path.pardir))

from storage.storageclient import StorageClient


class TestClient(unittest.TestCase):
    serv_host = "127.0.0.1"
    serv_port = 9090
    client = StorageClient(serv_host, serv_port, debug=False,
                           blocking=False, with_checker=False)

    @classmethod
    def setUpClass(cls) -> None:
        get_params = ["Bart", "Homer", "Lisa"]

        set_params = {
            "some_id": {
                "name": "Bart",
                "surname": "Simpson"
            },
            "another_id": "Lisa"
        }

        with open("correct_set_inp.json", 'w') as f:
            f.write(json.dumps(set_params))
        with open("correct_get_inp.json", 'w') as f:
            f.write(json.dumps(get_params))

    def test_prepare_set_data_from_raw_returns_correct_data(self):
        expected = {"db_name": "my_database",
                    "keys": [{"key": 'some_key',
                              "value": {'Sanic': 'fast'}}]}
        prepared_data = TestClient.client.prepare_set_data_from_raw(
            "my_database", 'some_key={"Sanic":"fast"}')
        assert expected["db_name"] == prepared_data["db_name"]
        self.assertListEqual(expected["keys"], prepared_data["keys"])

    def test_prepare_set_data_from_raw_raises_json_err_when_non_decoded_value(self):
        params = ["my_database", 'some_key={Sanic:fast}']
        self.assertRaises(JSONDecodeError,
                          TestClient.client.prepare_set_data_from_raw,
                          *params)

    def test_prepare_set_data_from_raw_raises_value_err_when_no_key_value_separator(self):
        params = ["my_database", 'some_key&{Sanic:fast}']
        self.assertRaises(ValueError,
                          TestClient.client.prepare_set_data_from_raw,
                          *params)

    def test_prepare_set_data_from_file_returns_correct_data(self):
        expected = {'db_name': 'my_database',
                    'keys': [
                        {'key': 'some_id',
                         'value': {'name': 'Bart', 'surname': 'Simpson'}},
                        {'key': 'another_id', 'value': 'Lisa'}]}

        prepared_data = TestClient.client.prepare_set_data_from_file(
            "my_database", "correct_set_inp.json")
        assert expected["db_name"] == prepared_data["db_name"]
        self.assertListEqual(expected["keys"], prepared_data["keys"])

    def test_prepare_set_data_from_file_raises_value_err_when_no_file(self):
        params = ["my_database", 'not_existing_file.json']
        self.assertRaises(ValueError,
                          TestClient.client.prepare_set_data_from_file,
                          *params)

    def test_prepare_get_data_from_raw_returns_correct_data(self):
        expected = {'db_name': 'my_database', 'keys': ["key1", "key2"]}
        prepared_data = TestClient.client.prepare_get_data_from_raw(
            "my_database", "key1&key2")
        assert expected["db_name"] == prepared_data["db_name"]
        self.assertListEqual(expected["keys"], prepared_data["keys"])

    def test_prepare_get_data_from_raw_raises_value_err_when_no_keys(self):
        params = ["my_database", ""]
        self.assertRaises(ValueError,
                          TestClient.client.prepare_get_data_from_raw,
                          *params)

    def test_prepare_get_data_from_file_returns_correct_data(self):
        expected = {'db_name': 'my_database',
                    'keys': ["Bart", "Homer", "Lisa"]}

        prepared_data = TestClient.client.prepare_get_data_from_file(
            "my_database", "correct_get_inp.json")
        assert expected["db_name"] == prepared_data["db_name"]
        self.assertListEqual(expected["keys"], prepared_data["keys"])

    def test_prepare_get_data_from_file_raises_value_err_when_no_file(self):
        params = ["my_database", 'not_existing_file.json']
        self.assertRaises(ValueError,
                          TestClient.client.prepare_get_data_from_file,
                          *params)

    @classmethod
    def tearDownClass(cls) -> None:
        if os.path.exists("./correct_set_inp.json"):
            os.remove("./correct_set_inp.json")
        if os.path.exists("./correct_get_inp.json"):
            os.remove("./correct_get_inp.json")


if __name__ == '__main__':
    unittest.main()
