import asyncio, socket
from broker import ResponseServerPacket, launch_producer, launch_consumer, Packet, command
import random
import json

PORT = 9000
CHUNK_LIMIT = 50
RESPONSE = 'HTTP/1.1 {status} {status_msg}\r\nContent-Type: text/html; charset=UTF-8\r\nContent-Encoding: UTF-8\r\nAccept-Ranges: bytes\r\nConnection: closed\r\n\r\n{html}'
events = {}
settings = {}


def load_settings():
    f = open('gw-settings.json')
    lines = f.read()
    return json.loads(lines)

class HttpResponse:
    def __init__(self, status: int, body) -> None:
        self.status = status
        self.body = body

    def get_status_desc(self):
        if self.status == 200:
            return 'OK'
        elif self.status == 404:
            return 'Not Found'
        else:
            return 'Internal Server Error'


@command(command_name='response_server', packet_type=ResponseServerPacket)
async def on_response(packet: Packet):
    data = events[packet.uid]
    data['data'] = HttpResponse(200, packet.data)
    data['event'].set()


@command(command_name='error_response', packet_type=ResponseServerPacket)
async def on_response_error(packet: Packet):
    data = events[packet.uid]
    if packet.data == 'route_not_found':
        data['data'] = HttpResponse(404, 'Route not found')
    else:
        data['data'] = HttpResponse(500, '')
    data['event'].set()


async def build_response(uid):
    packet = events[uid]
    data:HttpResponse = packet['data']
    response = RESPONSE.format(status=data.status,
                               status_msg= data.get_status_desc(),
                               html=data.body).encode('utf-8')

    return response


async def read_request(client):
    request = ''
    while True:
        chunk = (await loop.sock_recv(client, CHUNK_LIMIT)).decode('utf8')
        request += chunk
        if len(chunk) < CHUNK_LIMIT:
            break

    header, body = request.split('\r\n\r\n')
    lines = header.split('\r\n')
    method, url, _ = lines[0].split(' ')
    path, query = url.split('?')
    method = method.lower()
    module = path[:path[1:].index('/') + 2]
    path = path[len(module) - 1:]

    query_data = {}

    for record in query.split('&'):
        tmp = record.split('=')
        query_data[tmp[0]] = tmp[1]

    return Packet(str(random.random()), method + '@' + path,
                  query_data), module


async def handle_client(client, loop):
    request, module = await read_request(client)
    event = asyncio.Event()
    record = {'event': event, 'data': None}
    events[request.uid] = record

    if module in settings['services']:
        await launch_producer(settings['services'][module], request)
        await event.wait()
    else:
        record['data'] = HttpResponse(404, 'Route not found')
    response = await build_response(request.uid)
    await loop.sock_sendall(client, response)
    client.close()


async def run_server(selected_server, loop):
    print('Web server initiated')
    while True:
        client, _ = await loop.sock_accept(selected_server)
        loop.create_task(handle_client(client, loop))


async def main(server, loop):
    f1 = loop.create_task(run_server(server, loop))
    f2 = loop.create_task(launch_consumer('request_gateway', loop))
    await asyncio.wait([f1, f2])


settings = load_settings()

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server.bind(('localhost', PORT))
server.listen(1)
server.setblocking(False)

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
try:
    loop.run_until_complete(main(server, loop))
    loop.close()
except KeyboardInterrupt:
    server.close()
