import argparse
import asyncio
import functools
import logging
import logging.config
import os
import signal
import time
import uvloop
import aiohttp
import yaml

from typing import Dict, Callable, Optional
from aiohttp import web
from chia_rs import AugSchemeMPL, G2Element

from chia.protocols.pool_protocol import (
    PoolErrorCode,
    GetFarmerResponse,
    GetPoolInfoResponse,
    PostPartialRequest,
    PostFarmerRequest,
    PutFarmerRequest,
    validate_authentication_token,
    POOL_PROTOCOL_VERSION,
    AuthenticationPayload,
)
from chia.types.blockchain_format.sized_bytes import bytes32
from chia.util.byte_types import hexstr_to_bytes
from chia.util.hash import std_hash
from chia.util.json_util import obj_to_response
from chia.util.ints import uint8, uint64, uint32

from .record import FarmerRecord
from .pool import Pool
from .util import error_response, RequestMetadata

plogger = logging.getLogger('partials')


def allow_cors(response: web.Response) -> web.Response:
    response.headers["Access-Control-Allow-Origin"] = "*"
    return response


def check_authentication_token(launcher_id: bytes32, token: uint64, timeout: uint8) -> Optional[web.Response]:
    if not validate_authentication_token(token, timeout):
        return error_response(
            PoolErrorCode.INVALID_AUTHENTICATION_TOKEN,
            f"authentication_token {token} invalid for farmer {launcher_id.hex()}.",
        )
    return None


class PoolServer:
    def __init__(self, pool_config_path: str):

        # We load our configurations from here
        with open(pool_config_path) as f:
            pool_config: Dict = yaml.safe_load(f)
            pool_config['__path__'] = os.path.abspath(pool_config_path)

        self.log = logging.getLogger(__name__)
        self.pool = Pool(pool_config)

        self.pool_config = pool_config
        self.host = pool_config["server"]["server_host"]
        self.port = int(pool_config["server"]["server_port"])

    async def start(self):
        await self.pool.start()

    async def stop(self):
        await self.pool.stop()

    def wrap_http_handler(self, f) -> Callable:
        async def inner(request) -> aiohttp.web.Response:
            try:
                res_object = await f(request)
                if res_object is None:
                    res_object = {}
            except Exception as e:
                self.log.warning('Error while handling message', exc_info=True)
                if len(e.args) > 0:
                    res_error = error_response(PoolErrorCode.SERVER_EXCEPTION, f"{e.args[0]}")
                else:
                    res_error = error_response(PoolErrorCode.SERVER_EXCEPTION, f"{e}")
                return allow_cors(res_error)

            return allow_cors(res_object)

        return inner

    async def index(self, _) -> web.Response:
        return web.Response(text="OpenChia.io pool")

    async def get_pool_info(self, _) -> web.Response:
        res: GetPoolInfoResponse = GetPoolInfoResponse(
            self.pool.info_name,
            self.pool.info_logo_url,
            uint64(self.pool.min_difficulty),
            uint32(self.pool.relative_lock_height),
            POOL_PROTOCOL_VERSION,
            str(self.pool.pool_fee),
            self.pool.info_description,
            self.pool.wallets[0]['puzzle_hash'],
            self.pool.authentication_token_timeout,
        )
        return obj_to_response(res)

    async def get_farmer(self, request_obj) -> web.Response:
        # TODO(pool): add rate limiting
        launcher_id: bytes32 = hexstr_to_bytes(request_obj.rel_url.query["launcher_id"])
        authentication_token = uint64(request_obj.rel_url.query["authentication_token"])

        authentication_token_error: Optional[web.Response] = check_authentication_token(
            launcher_id, authentication_token, self.pool.authentication_token_timeout
        )
        if authentication_token_error is not None:
            return authentication_token_error

        farmer_record: Optional[FarmerRecord] = await self.pool.store.get_farmer_record(launcher_id)
        if farmer_record is None:
            return error_response(
                PoolErrorCode.FARMER_NOT_KNOWN, f"Farmer with launcher_id {launcher_id.hex()} unknown."
            )

        if farmer_record.singleton_tip_state.target_puzzle_hash in self.pool.default_target_puzzle_hashes:
            target_puzzle_hash = farmer_record.singleton_tip_state.target_puzzle_hash
        else:
            target_puzzle_hash = self.pool.default_target_puzzle_hashes[0]

        # Validate provided signature
        signature: G2Element = G2Element.from_bytes(hexstr_to_bytes(request_obj.rel_url.query["signature"]))
        message: bytes32 = std_hash(
            AuthenticationPayload(
                "get_farmer",
                launcher_id,
                target_puzzle_hash,
                authentication_token,
            )
        )
        if not AugSchemeMPL.verify(farmer_record.authentication_public_key, message, signature):
            return error_response(
                PoolErrorCode.INVALID_SIGNATURE,
                f"Failed to verify signature {signature} for launcher_id {launcher_id.hex()}.",
            )

        response: GetFarmerResponse = GetFarmerResponse(
            farmer_record.authentication_public_key,
            farmer_record.payout_instructions,
            farmer_record.difficulty,
            farmer_record.points,
        )

        self.pool.log.debug(f"get_farmer response {response.to_json_dict()}, " f"launcher_id: {launcher_id.hex()}")
        return obj_to_response(response)

    def post_metadata_from_request(self, request_obj):
        return RequestMetadata(
            url=str(request_obj.url),
            scheme=request_obj.scheme,
            headers=request_obj.headers,
            cookies=dict(request_obj.cookies),
            query=dict(request_obj.query),
            remote=request_obj.headers.get('x-forwarded-for') or request_obj.remote,
        )

    async def post_farmer(self, request_obj) -> web.Response:
        # TODO(pool): add rate limiting
        post_farmer_request: PostFarmerRequest = PostFarmerRequest.from_json_dict(await request_obj.json())

        authentication_token_error = check_authentication_token(
            post_farmer_request.payload.launcher_id,
            post_farmer_request.payload.authentication_token,
            self.pool.authentication_token_timeout,
        )
        if authentication_token_error is not None:
            return authentication_token_error

        post_farmer_response = await self.pool.add_farmer(
            post_farmer_request, self.post_metadata_from_request(request_obj))

        self.pool.log.info(
            f"post_farmer response {post_farmer_response}, "
            f"launcher_id: {post_farmer_request.payload.launcher_id.hex()}",
        )
        return obj_to_response(post_farmer_response)

    async def put_farmer(self, request_obj) -> web.Response:
        # TODO(pool): add rate limiting
        put_farmer_request: PutFarmerRequest = PutFarmerRequest.from_json_dict(await request_obj.json())

        authentication_token_error = check_authentication_token(
            put_farmer_request.payload.launcher_id,
            put_farmer_request.payload.authentication_token,
            self.pool.authentication_token_timeout,
        )
        if authentication_token_error is not None:
            return authentication_token_error

        put_farmer_response = await self.pool.update_farmer(
            put_farmer_request, self.post_metadata_from_request(request_obj)
        )

        return obj_to_response(put_farmer_response)

    async def post_partial(self, request_obj) -> web.Response:
        # TODO(pool): add rate limiting
        start_time = time.time()
        request = await request_obj.json()
        try:
            partial: PostPartialRequest = PostPartialRequest.from_json_dict(request)
        except ValueError as e:
            plogger.error('Failed to load partial: %r: %s', request, e)


        authentication_token_error = check_authentication_token(
            partial.payload.launcher_id,
            partial.payload.authentication_token,
            self.pool.authentication_token_timeout,
        )
        if authentication_token_error is not None:
            return authentication_token_error

        farmer_record: Optional[FarmerRecord] = await self.pool.store.get_farmer_record(partial.payload.launcher_id)
        if farmer_record is None:
            return error_response(
                PoolErrorCode.FARMER_NOT_KNOWN,
                f"Farmer with launcher_id {partial.payload.launcher_id.hex()} not known.",
            )

        post_partial_response: Dict = await self.pool.process_partial(
            partial,
            farmer_record,
            self.post_metadata_from_request(request_obj),
            uint64(int(start_time)),
        )

        plogger.info(
            f"post_partial response {post_partial_response}, time: {time.time() - start_time} "
            f"launcher_id: {request['payload']['launcher_id']}"
        )
        return obj_to_response(post_partial_response)

    async def get_login(self, request_obj) -> web.Response:
        # Redirect to website
        raise aiohttp.web.HTTPFound(self.pool.pool_config["login_url"] + '?' + request_obj.url.query_string)

    async def switch_node(self):
        try:
            self.pool.set_healthy_node(switch=True)
        except Exception:
            plogger.error('Failed to switch node', exc_info=True)


server: Optional[PoolServer] = None
runner: Optional[aiohttp.web.BaseRunner] = None
run_forever_task = None


async def start_pool_server(pool_config_path=None):
    global server
    global runner
    global run_forever_task
    server = PoolServer(pool_config_path)
    await server.start()

    app = web.Application()
    app.add_routes(
        [
            web.get("/", server.wrap_http_handler(server.index)),
            web.get("/pool_info", server.wrap_http_handler(server.get_pool_info)),
            web.get("/farmer", server.wrap_http_handler(server.get_farmer)),
            web.post("/farmer", server.wrap_http_handler(server.post_farmer)),
            web.put("/farmer", server.wrap_http_handler(server.put_farmer)),
            web.post("/partial", server.wrap_http_handler(server.post_partial)),
            web.get("/login", server.get_login),
        ]
    )
    runner = aiohttp.web.AppRunner(app, access_log=None)
    await runner.setup()
    site = aiohttp.web.TCPSite(
        runner,
        host=server.host,
        port=server.port,
    )
    await site.start()

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, functools.partial(stop_sync, loop))
    loop.add_signal_handler(signal.SIGUSR1, functools.partial(server_switch_node, server, loop))

    async def run_forever():
        while True:
            await asyncio.sleep(3600)

    run_forever_task = asyncio.create_task(run_forever())
    try:
        await run_forever_task
    except asyncio.exceptions.CancelledError:
        pass


def stop_sync(loop):
    asyncio.run_coroutine_threadsafe(stop(), loop)


def server_switch_node(server, loop):
    asyncio.run_coroutine_threadsafe(server.switch_node(), loop)


async def stop():
    await server.stop()
    await runner.cleanup()
    if run_forever_task:
        run_forever_task.cancel()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', default=f'{os.getcwd()}/config.yaml')
    parser.add_argument('--log-level', default='INFO')
    parser.add_argument('--log-dir')

    args = parser.parse_args()

    logging.root.setLevel(getattr(logging, args.log_level))

    handlers = ["console"]
    if args.log_dir:
        main_handlers = handlers + ['file', 'file_json']
        partial_handlers = handlers + ['partial', 'partial_json']
    else:
        main_handlers = partial_handlers = handlers

    logging.config.dictConfig({
        "version": 1,
        "disable_existing_loggers": False,
        "loggers": {
            "partials": {
                "handlers": partial_handlers,
                "propagate": False,
            },
            "": {
                "handlers": main_handlers,
            },
        },
        "handlers": {
            "console": {
                "level": args.log_level,
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
                "formatter": "colored",
            },
            "file": {
                "level": args.log_level,
                "class": "logging.handlers.RotatingFileHandler",
                "filename": (
                    os.path.join(args.log_dir, 'main.log')
                    if args.log_dir else
                    '/dev/null'
                ),
                "maxBytes": 5 * 1024 * 1024,
                "backupCount": 5,
                "formatter": "colored",
            },
            "file_json": {
                "level": args.log_level,
                "class": "logging.handlers.RotatingFileHandler",
                "filename": (
                    os.path.join(args.log_dir, 'main.log.json')
                    if args.log_dir else
                    '/dev/null'
                ),
                "maxBytes": 5 * 1024 * 1024,
                "backupCount": 5,
                "formatter": "json",
            },
            "partial": {
                "level": args.log_level,
                "class": "logging.handlers.RotatingFileHandler",
                "filename": (
                    os.path.join(args.log_dir, 'partial.log')
                    if args.log_dir else
                    '/dev/null'
                ),
                "maxBytes": 5 * 1024 * 1024,
                "backupCount": 5,
                "formatter": "colored",
            },
            "partial_json": {
                "level": args.log_level,
                "class": "logging.handlers.RotatingFileHandler",
                "filename": (
                    os.path.join(args.log_dir, 'partial.log.json')
                    if args.log_dir else
                    '/dev/null'
                ),
                "maxBytes": 5 * 1024 * 1024,
                "backupCount": 5,
                "formatter": "json",
            },
        },
        "formatters": {
            "colored": {
                "()": "colorlog.ColoredFormatter",
                "format": "[%(asctime)s] (%(log_color)s%(levelname)-8s%(reset)s) %(name)s.%(funcName)s():%(lineno)d - %(message)s",
            },
            "json": {
                "()": "pythonjsonlogger.jsonlogger.JsonFormatter",
                "format": "%(message)s %(levelname)s %(name)s %(funcName)s %(lineno)d",
                "timestamp": True,
            },
        }
    })

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

    try:
        asyncio.run(
            start_pool_server(pool_config_path=args.config),
            debug=args.log_level == 'DEBUG',
        )
    except KeyboardInterrupt:
        asyncio.run(stop())


if __name__ == "__main__":
    main()
