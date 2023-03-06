import asyncio
import json
import aio_pika


class __CommandDecorator:

    def __init__(self) -> None:
        self.registry = {}

    def __call__(self, command_name: str, packet_type):

        def decorator_call(func):
            self.registry[command_name] = func

            def wrapper(*args, **kwargs):
                func(args)

            return wrapper

        return decorator_call


command = __CommandDecorator()


class Packet:

    def __init__(self, uid: str, command: str, data) -> None:
        self.uid = uid
        self.command = command
        self.data = data

    def get(self, model_class):
        return model_class(**self.data)


class ResponseServerPacket(Packet):
    SUCCESS = 'success'
    FAILED = 'failed'

    def __init__(self, uid: str, command: str, data) -> None:
        super().__init__(uid, command, data)


async def launch_consumer(queue_name, loop):
    print('Consumer initiated')
    connection = await aio_pika.connect_robust("amqp://guest:guest@127.0.0.1/",
                                               loop=loop)

    async with connection:
        channel: aio_pika.abc.AbstractChannel = await connection.channel()

        queue: aio_pika.abc.AbstractQueue = await channel.declare_queue(
            queue_name, auto_delete=False)

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    tmp = json.loads(message.body.decode('utf8'))
                    if tmp['command'] in command.registry:
                        await command.registry[tmp['command']](Packet(**tmp))
                    else:
                        await launch_producer(
                            'request_gateway',
                            Packet(tmp['uid'], 'error_response',
                                   'route_not_found'))


async def launch_producer(queue: str, packet):
    loop = asyncio.get_event_loop()
    connection = await aio_pika.connect_robust("amqp://guest:guest@127.0.0.1/",
                                               loop=loop)

    routing_key = queue

    channel: aio_pika.abc.AbstractChannel = await connection.channel()

    await channel.default_exchange.publish(
        aio_pika.Message(body=json.dumps(packet.__dict__).encode()),
        routing_key=routing_key)

    await connection.close()
