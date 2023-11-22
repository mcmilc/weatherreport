from os import walk
import pycurl
from io import BytesIO


class WeatherAPI:
    def __init__(self, client=pycurl.Curl()):
        """
        :param root_url (str) top url of REST API
        :param parameters (dict) key is param name, val is param
        """
        self._init_sep = "?"
        self._parameter_sep = "&"
        self.client = client

    def build_full_url(self, root_url: str, parameters: dict):
        url = root_url
        for i, key_value in enumerate(parameters.items()):
            if i == 0:
                sep = self._init_sep
            else:
                sep = self._parameter_sep
            param_name = key_value[0]
            param_val = key_value[1]
            url += sep + param_name + "=" + param_val
        return url

    def request(self, full_url):
        self.buffer = BytesIO()
        self.client.setopt(self.client.WRITEDATA, self.buffer)
        self.client.setopt(self.client.URL, full_url)
        self.client.perform()
        response = self.buffer.getvalue().decode("utf-8")
        self.buffer.close()
        return response
