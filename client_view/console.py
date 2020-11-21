import sys
from PyQt5 import QtGui
from PyQt5.QtWidgets import QPlainTextEdit
from PyQt5.QtGui import QPalette, QTextCharFormat
from PyQt5.QtCore import Qt, pyqtSignal, QObject


class EnterMessage(QObject):
    enter_message = pyqtSignal(str, name="onEnterMessage")

    def update(self, command):
        self.onEnterMessage.emit(command)


class Console(QPlainTextEdit):
    def __init__(self, parent):
        super().__init__(parent)
        self.prompt = "KVStorage> "
        self.palette = QPalette()
        self.history = []
        self.history_pos = 0
        self.initUI()

        self.is_locked = False
        self.enter_mes_signal = EnterMessage()

    def initUI(self):
        self.palette.setColor(QPalette.Base, Qt.black)
        self.palette.setColor(QPalette.Text, Qt.green)
        self.setPalette(self.palette)

        self.insert_prompt(False)

    def keyPressEvent(self, e: QtGui.QKeyEvent) -> None:
        if self.is_locked:
            return

        key = e.key()
        mod = e.modifiers()

        if key == Qt.Key_Backspace:
            if mod == Qt.NoModifier and \
                    self.textCursor().positionInBlock() > len(self.prompt):
                super().keyPressEvent(e)

        if key == Qt.Key_Return and mod == Qt.NoModifier:
            self.on_enter()

        if key == Qt.Key_Up and mod == Qt.NoModifier:
            self.back_history()

        if key == Qt.Key_Down and mod == Qt.NoModifier:
            self.forward_history()

        if 32 <= key <= 121 and \
                (mod == Qt.NoModifier or mod == Qt.ShiftModifier):
            super().keyPressEvent(e)

    def on_enter(self):
        if self.textCursor().positionInBlock() == len(self.prompt):
            self.insert_prompt(True)
            return
        query = self.textCursor().block().text()[len(self.prompt):]
        self.is_locked = True
        self.add_history(query)

        self.enter_mes_signal.update(query)
        self.print_output("command done")

    def print_output(self, output):
        self.textCursor().insertBlock()
        form = QTextCharFormat()
        form.setForeground(Qt.white)
        self.textCursor().setBlockCharFormat(form)
        self.textCursor().insertText(output)
        self.insert_prompt(True)
        self.is_locked = False

    def insert_prompt(self, insert_new_block: bool):
        if insert_new_block:
            self.textCursor().insertBlock()
        form = QTextCharFormat()
        form.setForeground(Qt.green)
        self.textCursor().setBlockCharFormat(form)
        self.textCursor().insertText(self.prompt)
        self.scroll_down()

    def scroll_down(self):
        bar = self.verticalScrollBar()
        bar.setValue(bar.maximum())

    def add_history(self, query):
        self.history.append(query)
        self.history_pos += 1

    def back_history(self):
        if not self.history_pos:
            return

        cursor = self.textCursor()
        cursor.movePosition(cursor.StartOfBlock)
        cursor.movePosition(cursor.EndOfBlock, cursor.KeepAnchor)
        cursor.removeSelectedText()
        cursor.insertText(self.prompt + self.history[self.history_pos - 1])
        self.setTextCursor(cursor)
        self.history_pos -= 1

    def forward_history(self):
        if self.history_pos == len(self.history):
            return

        cursor = self.textCursor()
        cursor.movePosition(cursor.StartOfBlock)
        cursor.movePosition(cursor.EndOfBlock, cursor.KeepAnchor)
        cursor.removeSelectedText()

        if self.history_pos == len(self.history) - 1:
            cursor.insertText(self.prompt)
        else:
            cursor.insertText(self.prompt + self.history[self.history_pos - 1])

        self.setTextCursor(cursor)
        self.history_pos += 1

    def mousePressEvent(self, e: QtGui.QMouseEvent) -> None:
        self.setFocus()

    def contextMenuEvent(self, e: QtGui.QContextMenuEvent) -> None:
        pass

    def mouseDoubleClickEvent(self, e: QtGui.QMouseEvent) -> None:
        pass
