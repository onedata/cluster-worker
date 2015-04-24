import json
import requests

appmock_rc_port = 9999


def _http_post(ip, port, path, use_ssl, data):
    """Helper function that perform a HTTP GET request
    Returns a tuple (Code, Headers, Body)
    """
    protocol = 'https' if use_ssl else 'http'
    response = requests.post(
        '{0}://{1}:{2}{3}'.format(protocol, ip, port, path),
        data, verify=False, timeout=10)
    return response.status_code, response.headers, response.text


def rest_endpoint_request_count(appmock_ip, endpoint_port, endpoint_path):
    """Returns how many times has given endpoint been requested.
    IMPORTANT: the endpoint_path must be literally the same as in app_desc
    module, for example: '/test1/[:binding]'
    """
    json_data = {
        'port': endpoint_port,
        'path': endpoint_path
    }
    _, _, body = _http_post(appmock_ip, appmock_rc_port,
                            '/rest_endpoint_request_count', True,
                            json.dumps(json_data))
    body = json.loads(body)
    if body['result'] == 'error':
        raise Exception(
            'rest_endpoint_request_count returned error: ' + body['reason'])
    return body['result']


def verify_rest_history(appmock_ip, expected_history):
    """Verifies if rest endpoints were requested in given order.
    Returns True or False.
    The expected_history is a list of tuples (port, path), for example:
    [(8080, '/test1/[:binding]'), (8080, '/test2')]
    """

    def create_endpoint_entry(_port, _path):
        entry = {
            'endpoint': {
                'path': _path,
                'port': _port
            }
        }
        return entry

    json_data = [create_endpoint_entry(port, path) for (port, path) in
                 expected_history]
    _, _, body = _http_post(appmock_ip, appmock_rc_port,
                            '/verify_rest_history', True,
                            json.dumps(json_data))
    body = json.loads(body)
    if body['result'] == 'error':
        raise Exception(
            'expected history does not match: ' + str(body['history']))
    return body['result']


def tcp_server_message_count(appmock_ip, tcp_port, message_binary):
    """Returns number of messages exactly mathing given message,
    that has been received by the TCP server mock.
    """
    _, _, body = _http_post(appmock_ip, appmock_rc_port,
                            '/tcp_server_message_count/' + str(tcp_port),
                            True, message_binary)
    body = json.loads(body)
    if body['result'] == 'error':
        raise Exception(
            'tcp_server_message_count returned error: ' + body['reason'])
    return body['result']


def tcp_server_send(appmock_ip, tcp_port, message_binary):
    """Orders appmock to send given message to all connected clients."""
    _, _, body = _http_post(appmock_ip, appmock_rc_port,
                            '/tcp_server_send/' + str(tcp_port),
                            True, message_binary)
    body = json.loads(body)
    if body['result'] == 'error':
        raise Exception('tcp_server_send returned error: ' + body['reason'])
    return body['result']
