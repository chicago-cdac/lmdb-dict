"""Thread debugging tools"""

import threading


class ThreadDebugger:
    """Get and set forced pauses in threads.

    Roughly modeled after pdb.set_trace, e.g.:

        tdb = ThreadDebugger()

        def in_thread():
            tdb.set_pause('in_thread')

        thread = Thread(target=in_thread)
        thread.start()

        pause = tdb.get_pause('in_thread')

        # if useful may wait until thread reaches its pause
        pause.await_waiting()

        # when we're ready we can clear the pause
        pause.clear()

    """
    @staticmethod
    def make_pause():
        return ThreadPause()

    def __init__(self):
        self.pauses = {}
        self._write_lock = threading.Lock()

    def get_pause(self, name):
        with self._write_lock:
            if name in self.pauses:
                return self.pauses[name]

            pause = self.pauses[name] = self.make_pause()
            return pause

    def set_pause(self, name, reset=True):
        pause = self.get_pause(name)

        pause.set()

        if reset:
            pause.reset()


class ThreadPause:

    def __init__(self):
        self._permitted = threading.Event()
        self._waiting = threading.Event()

    def set(self):
        if not self._permitted.is_set():
            self._waiting.set()

        self._permitted.wait()

    def reset(self):
        self._waiting.clear()
        self._permitted.clear()

    def clear(self):
        self._permitted.set()
        self._waiting.clear()

    @property
    def cleared(self):
        return self._permitted.is_set()

    @property
    def waiting(self):
        return self._waiting.is_set()

    def await_waiting(self):
        self._waiting.wait()
