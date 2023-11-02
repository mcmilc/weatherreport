from __future__ import print_function
import swagger_client
from swagger_client.rest import ApiException
from pprint import pprint


def create_config():
    configuration = swagger_client.Configuration()
    configuration.api_key["key"] = "44a4177b6e0f4eab9bd33518231310"
    return configuration


def create_api_client(config):
    return swagger_client.APIsApi(swagger_client.ApiClient(config))


def connect_to_service(api_client, q, dt):
    try:
        # Astronomy API
        api_response = api_client.astronomy(q, dt)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling APIsApi->astronomy: %s\n" % e)


def test():
    config = create_config()
    client = create_api_client(config)
    connect_to_service(client, "90250", "10-12-2023")


if __name__ == "__main__":
    test()
