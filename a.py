import sys
import os
from broker import launch_consumer, launch_producer, Packet, command
import asyncio


@command(command_name='get@/hi/', packet_type=Packet)
async def on_hi(packet):
    await launch_producer(
        'request_gateway', Packet(packet.uid, 'response_server',
                                  'hello world!'))


@command(command_name='get@/bye/', packet_type=Packet)
async def on_bye(packet):
    await launch_producer(
        'request_gateway', Packet(packet.uid, 'response_server',
                                  'bye world!'))


loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

if __name__ == '__main__':
    try:
        loop.run_until_complete(launch_consumer('request_a', loop))
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
