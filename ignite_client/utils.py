import threading


class AtomicInteger:
    def __init__(self, initial_value=0):
        self.value = initial_value
        self.lock = threading.Lock()

    def increment(self):
        with self.lock:
            self.value += 1
            return self.value

    def get(self):
        with self.lock:
            return self.value
