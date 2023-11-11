import os
import select

class CommonSelectableSignal:

    def __init__(self):
        self._read_fd = None
        self._write_fd = None

    def push(self):
        pass

    def pop(self):
        pass

    def fileno(self):
        return self._read_fd

class LinuxCommonSelectableSignal(CommonSelectableSignal):

    _DATA = b"A"

    def __init__(self):
        super().__init__()
        self._read_fd, self._write_fd = os.pipe()

    def push(self):
        os.write(self._write_fd, LinuxCommonSelectableSignal._DATA)

    def pop(self):
        os.read(self._read_fd, 1000)

SelectableSignal = LinuxCommonSelectableSignal

