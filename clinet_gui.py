import sys
import os
import json
import threading
import time
from PyQt5 import QtGui, QtCore, QtWidgets
from PyQt5.QtWidgets import *
from PyQt5.QtGui import *
from PyQt5.QtCore import *

from client_view.console import Console
from storage.storageclient import StorageClient


class Window(QWidget):
    def __init__(self):
        super().__init__()
        self.title = "KVStorage client"
        self.window_width = 1024
        self.window_height = 600

        self.cons = Console(self)
        self.cons.setGeometry(0, self.window_height // 2, self.window_width, self.window_height // 2)
        self.cons.enter_mes_signal.onEnterMessage.connect(self.on_console)

        self.client = self.init_client()
        self.client_thread = threading.Thread(target=self.client.run)

        self.result_message_box = QPlainTextEdit(self)
        self.service_message_box = QPlainTextEdit(self)

        self.initUI()
        self.client_thread.start()

    def init_client(self):
        conf_path = StorageClient.config_path
        if not os.path.exists(conf_path):
            self.show_err_message_and_exit(
                f"Не найден файл конфигурации {conf_path}")
        with open(conf_path, "r") as f:
            conf = json.loads(f.read())

        client = StorageClient(
            conf["cluster_node_host"],
            conf["cluster_node_port"],
            with_checker=conf["with_checker"],
            debug=conf["debug"],
            blocking=False)

        self.connect_handlers_to_client_signals(client)
        return client

    def connect_handlers_to_client_signals(self, client):
        client.err_signal.onError.connect(self.show_err_message_and_exit)
        client.service_signal.onService.connect(self.on_service_client_mes)

    def on_service_client_mes(self, message):
        self.service_message_box.textCursor().insertBlock()
        self.service_message_box.textCursor().insertText(message)

    def show_err_message_and_exit(self, message):
        QMessageBox.warning(
            self, "Message - KVStorage", message,
            QMessageBox.Ok, QMessageBox.Ok)
        self.closeEvent(QCloseEvent)
        sys.exit(1)

    def initUI(self):
        self.setWindowTitle(self.title)
        self.setGeometry(300, 300, self.window_width, self.window_height)
        self.setFixedSize(self.window_width, self.window_height)
        self.result_message_box.setGeometry(self.window_width // 2, 0,
                                            self.window_width // 2,
                                            self.window_height // 2)

        self.service_message_box.setGeometry(0, 0,
                                             self.window_width // 2,
                                             self.window_height // 2)
        self.show()

    def print_client_req_result(self, output):
        self.result_message_box.textCursor().insertBlock()
        self.result_message_box.textCursor().insertText(output)
        self.result_message_box.textCursor().insertBlock()
        self.result_message_box.textCursor().insertBlock()

    def on_console(self, command: str):
        response = self.client.handle_command(command)
        if response is not None:
            self.print_client_req_result(json.dumps(response.json(), indent=2))

    def closeEvent(self, QCloseEvent):
        if hasattr(self, "client"):
            self.client.exit()
        if hasattr(self, "client_thread"):
            self.client_thread.join()


if __name__ == '__main__':
    app = QApplication(sys.argv)
    ex = Window()
    ex.show()
    sys.exit(app.exec_())
