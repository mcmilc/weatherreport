class TIMock:
    def __init__(self):
        self._data = {}

    def xcom_push(self, key, val):
        self._data[key] = val

    def xcom_pull(self, key, task_ids=None):
        return self._data[key]
