import asyncio
from collections.abc import Callable, Awaitable
from datetime import datetime, timezone
import dataclasses
import enum
import logging
from typing import Any
import uuid


import msgspec


from .job_spec import JobError, JobSpec
from .jsonrpc_schemas import (
    RpcMsgMeta,
    RpcRequest,
    RpcResponse,
    RpcErrorField,
    RpcErrorResponse,
    RpcNotification,
    JsonRpcRequestSchema,
    JsonRpcResponseSchema,
    JsonRpcNotificationSchema,
    JsonRpcErrorResponseSchema,
)


class MsgTypes(enum.Enum):
    REQUEST = "rpc_request"
    RESPONSE = "rpc_response"
    ERROR_RESPONSE = "rpc_error_response"
    NOTIFICATION = "rpc_notification"
    UNKNOWN = "rpc_unknown"


class MsgSpecRpcCommonFields(msgspec.Struct):
    """A schema used for initially determining the msg type"""

    id: str = None
    method: str = None
    error: dict = None


def detect_rpc_msg_type(msg: MsgSpecRpcCommonFields) -> MsgTypes:
    if msg.id is None:
        if msg.method is None:
            return MsgTypes.UNKNOWN
        return MsgTypes.NOTIFICATION
    if msg.method is None:
        if msg.error is not None:
            return MsgTypes.ERROR_RESPONSE
        return MsgTypes.RESPONSE
    return MsgTypes.REQUEST


@dataclasses.dataclass
class RpcMethodDef:
    name: str
    request_schema: Any = None
    response_schema: Any = None
    coro: Callable[[Any], Awaitable[Any]] = None
    job_queue: asyncio.Queue = None


@dataclasses.dataclass
class RpcChannelDef:
    name: str
    notification_schema: Any = None
    queue: asyncio.Queue = None


@dataclasses.dataclass
class RpcGenericMsg:
    msg_json: str
    msg_type: enum.Enum
    context_id: str = None
    method: str = None
    error: dict = None


@dataclasses.dataclass
class CallerSentRequests:
    context_id: str
    method_name: str
    params: dict
    future: asyncio.Future


class RpcError(Exception):
    def __init__(self, error_field: RpcErrorField) -> None:
        self.rpc_code = error_field.code
        self.rpc_message = error_field.message
        self.rpc_data = error_field.data
        super().__init__(error_field.message)


class RpcPeer:
    incoming_queue: asyncio.Queue[str]
    outgoing_queue: asyncio.Queue[str]
    passthrough_queue: asyncio.Queue[str]

    callee_methods: dict[str, RpcMethodDef]

    consumer_channels: dict[str, RpcChannelDef]
    producer_channels: dict[str, RpcChannelDef]

    sent_rpc_requests: dict[str, CallerSentRequests]
    received_rpc_requests: dict[str, Any]

    handlers_by_msg_type: dict[MsgTypes, Callable[[RpcGenericMsg], None]]

    default_job_queue: asyncio.Queue[JobSpec]
    _requests_by_job_future: dict[asyncio.Future, RpcGenericMsg]

    def __init__(
        self,
        *,
        default_job_queue: asyncio.Queue = None,
        incoming_queue: asyncio.Queue = None,
        outgoing_queue: asyncio.Queue = None,
        passthrough_queue: asyncio.Queue = None,
    ) -> None:
        if default_job_queue is not None:
            self.default_job_queue = default_job_queue
        else:
            self.default_job_queue = asyncio.Queue()
        self._logger = logging.getLogger()
        self._default_consumer_queue = asyncio.Queue()
        self._default_producer_queue = asyncio.Queue()
        self.n_decode_errors = 0
        if incoming_queue is None:
            incoming_queue = asyncio.Queue()
        self.incoming_queue = incoming_queue
        if outgoing_queue is None:
            outgoing_queue = asyncio.Queue()
        self.outgoing_queue = outgoing_queue
        self.passthrough_queue = passthrough_queue
        self.callee_methods = {}
        self.consumer_channels = {}
        self.producer_channels = {}
        self.sent_rpc_requests = {}
        self.received_rpc_requests = {}
        self._loop = asyncio.get_running_loop()
        self._generic_decoder = msgspec.json.Decoder(type=MsgSpecRpcCommonFields)
        self._requests_by_job_future = {}

        self.handlers_by_msg_type = {
            MsgTypes.ERROR_RESPONSE: self._handle_error_response,
            MsgTypes.RESPONSE: self._handle_response,
            MsgTypes.REQUEST: self._callee_handle_request,
            MsgTypes.NOTIFICATION: self._handle_incoming_notify,
            MsgTypes.UNKNOWN: self._handle_unknown,
        }

    def register_method(self, name, coro, job_queue: asyncio.Queue = None) -> bool:
        if name in self.callee_methods:
            return False
        if job_queue is None:
            job_queue = self.default_job_queue

        self.callee_methods[name] = RpcMethodDef(
            name=name,
            request_schema=None,
            response_schema=None,
            coro=coro,
            job_queue=job_queue,
        )
        return True

    async def call_rpc_method(self, method_name: str, params: Any = None) -> Any:
        if params is None:
            params = {}

        context_id = str(uuid.uuid4())
        message_date = datetime.now(timezone.utc)
        request = RpcRequest(context_id, method_name, params, RpcMsgMeta(message_date))
        try:
            request_json = JsonRpcRequestSchema().dumps(request)
        except Exception as e:
            raise RuntimeError from e

        await self.outgoing_queue.put(request_json)

        future = asyncio.get_running_loop().create_future()

        self.sent_rpc_requests[context_id] = CallerSentRequests(
            context_id, method_name, params, future
        )
        return await future

    async def call(self, method_name: str, **kwargs) -> Any:
        return await self.call_rpc_method(method_name, params=kwargs)

    def _handle_response(self, response_msg: RpcGenericMsg) -> None:
        if response_msg.context_id not in self.sent_rpc_requests:
            # unknown request
            return
        sent_rpc_request = self.sent_rpc_requests[response_msg.context_id]

        response: RpcResponse = JsonRpcResponseSchema().loads(response_msg.msg_json)
        sent_rpc_request.future.set_result(response.result)

    def _handle_error_response(self, error_response_msg: RpcGenericMsg) -> None:
        if error_response_msg.context_id not in self.sent_rpc_requests:
            # unknown request
            return
        sent_rpc_request = self.sent_rpc_requests[error_response_msg.context_id]

        error_response: RpcErrorResponse = JsonRpcErrorResponseSchema().loads(
            error_response_msg.msg_json
        )
        sent_rpc_request.future.set_exception(RpcError(error_response.error))

    def register_producer(self, name: str, queue: asyncio.Queue = None) -> bool:
        if name in self.producer_channels:
            return False

        if queue is None:
            queue = self._default_producer_queue

        self.producer_channels[name] = RpcChannelDef(
            name=name,
            queue=queue,
        )
        return True

    def register_consumer(self, name: str, queue: asyncio.Queue = None) -> bool:
        if name in self.consumer_channels:
            return False

        if queue is None:
            queue = self._default_consumer_queue

        self.consumer_channels[name] = RpcChannelDef(
            name=name,
            queue=queue,
        )
        return True

    async def loop_process_incoming(self) -> None:
        while True:
            msg_json = await self.incoming_queue.get()
            generic_msg = self._prep_generic_msg_obj(msg_json)
            if generic_msg is None:
                self.n_decode_errors += 1
                continue
            self.route_by_type(generic_msg)

    async def publish_loop(self, channel_name: str) -> None:
        if channel_name not in self.producer_channels:
            return
        channel = self.producer_channels[channel_name]
        notification_schema = JsonRpcNotificationSchema()
        while True:
            frame = await channel.queue.get()
            msg_json = notification_schema.dumps(
                RpcNotification(method=channel_name, params=frame)
            )
            await self.outgoing_queue.put(msg_json)

    def publish_one(self, channel_name: str, frame: Any) -> None:
        if channel_name not in self.producer_channels:
            return
        channel = self.producer_channels[channel_name]
        notification_schema = JsonRpcNotificationSchema()
        msg_json = notification_schema.dumps(
            RpcNotification(method=channel_name, params=frame)
        )
        self.outgoing_queue.put_nowait(msg_json)

    def _prep_generic_msg_obj(self, msg_json: str) -> RpcGenericMsg:
        try:
            generic_fields = self._generic_decoder.decode(msg_json)
        except msgspec.MsgspecError as _exc:
            return None
        msg_type = detect_rpc_msg_type(generic_fields)
        generic_msg = RpcGenericMsg(
            msg_json=msg_json,
            msg_type=msg_type,
            context_id=generic_fields.id,
            method=generic_fields.method,
            error=generic_fields.error,
        )
        return generic_msg

    def route_by_type(self, generic_msg: RpcGenericMsg) -> None:
        self.handlers_by_msg_type[generic_msg.msg_type](generic_msg)

    def _handle_incoming_notify(self, notify_msg: RpcGenericMsg) -> None:
        if notify_msg.method not in self.consumer_channels:
            # unknown notification type
            return
        notification_schema = JsonRpcNotificationSchema()
        channel = self.consumer_channels[notify_msg.method]
        notification = notification_schema.loads(notify_msg.msg_json)
        channel.queue.put_nowait(notification["params"])

    def _handle_unknown(self, unknown_msg: RpcGenericMsg) -> None:
        log_msg = f"Cannot determine message type:\n{unknown_msg.msg_json}"
        self._logger.error(log_msg)
        if self.passthrough_queue:
            self.passthrough_queue.put_nowait(unknown_msg)

    def _callee_handle_request(self, request_msg: RpcGenericMsg) -> None:
        log_msg = f"callee_handle_request: {request_msg}"
        self._logger.debug(log_msg)
        if request_msg.context_id in self.received_rpc_requests:
            # ignoring a possibly repeated request
            return
        if request_msg.method not in self.callee_methods:
            message_date = datetime.now(timezone.utc)
            response = RpcErrorResponse(
                request_msg.context_id,
                RpcErrorField(500, "unknown_method", {}),
                RpcMsgMeta(message_date),
            )
            response_json = JsonRpcErrorResponseSchema().dumps(response)
            self.outgoing_queue.put_nowait(response_json)
            return
        method = self.callee_methods[request_msg.method]
        try:
            request: RpcRequest = JsonRpcRequestSchema().loads(request_msg.msg_json)
        except Exception as exc:
            message_date = datetime.now(timezone.utc)
            response = RpcErrorResponse(
                request_msg.context_id,
                RpcErrorField(
                    500, "param_validation_error", {"validation_error": str(exc)}
                ),
                RpcMsgMeta(message_date),
            )
            response_json = JsonRpcErrorResponseSchema().dumps(response)
            self.outgoing_queue.put_nowait(response_json)
            return

        if request.params is not None:
            params = request.params
        else:
            params = {}

        future = self._loop.create_future()
        future.add_done_callback(self._callee_handle_job_outcome)
        job = JobSpec(
            name=request_msg.context_id,
            coro=method.coro,
            params=params,
            future=future,
        )
        self._requests_by_job_future[future] = request_msg
        method.job_queue.put_nowait(job)

    def _callee_handle_job_outcome(self, _future: asyncio.Future) -> None:
        message_date = datetime.now(timezone.utc)
        request: RpcGenericMsg = self._requests_by_job_future[_future]
        del self._requests_by_job_future[_future]
        if _future.cancelled():
            response = RpcErrorResponse(
                request.context_id,
                RpcErrorField(500, "cancelled", {}),
                RpcMsgMeta(message_date),
            )
            response_json = JsonRpcErrorResponseSchema().dumps(response)
            self.outgoing_queue.put_nowait(response_json)
            return

        exc: JobError = _future.exception()
        if exc is not None:
            response = RpcErrorResponse(
                request.context_id,
                RpcErrorField(500, str(exc), {"coro_exc": str(exc.coro_exc)}),
                RpcMsgMeta(message_date),
            )
            response_json = JsonRpcErrorResponseSchema().dumps(response)
            self.outgoing_queue.put_nowait(response_json)
            return

        # reply with RpcResponse
        response = RpcResponse(
            context_id=request.context_id,
            result=_future.result(),
            msg_meta=RpcMsgMeta(message_date),
        )
        response_json = JsonRpcResponseSchema().dumps(response)
        self.outgoing_queue.put_nowait(response_json)
