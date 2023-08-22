from dataclasses import dataclass
from datetime import datetime
from typing import Any


from marshmallow import Schema, fields, validate
from marshmallow import post_dump, post_load


class ConstField(fields.String):
    _const_value: str

    def __init__(self, const_value: str, **kwargs) -> None:
        self._const_value = const_value
        kwargs["dump_default"] = self._const_value
        kwargs["validate"] = validate.OneOf([self._const_value])
        super().__init__(**kwargs)


class OrderedCamelCaseSchema(Schema):
    """Schema that uses camel-case for its external representation
    and snake-case for its internal representation.
    """

    class Meta:
        ordered = True

    @staticmethod
    def _camelcase(key):
        parts = iter(key.split("_"))
        return next(parts) + "".join(i.title() for i in parts)

    def on_bind_field(self, field_name, field_obj):
        field_obj.data_key = self._camelcase(field_obj.data_key or field_name)


@dataclass
class RpcMsgMeta:
    message_date: datetime = None
    reception_date: datetime = None


class JsonRpcMsgMetaPartial(OrderedCamelCaseSchema):
    message_date = fields.DateTime()
    reception_date = fields.DateTime()

    @post_load
    def make_msg_meta_object(self, data: dict, **_kwargs) -> RpcMsgMeta:
        message_date = None
        if "message_date" in data:
            message_date = data["message_date"]
        reception_date = None
        if "reception_date" in data:
            reception_date = data["reception_date"]
        return RpcMsgMeta(message_date, reception_date)

    @post_dump
    def _exclude_empty_dates(self, data: dict, **_kwargs) -> dict:
        optional_param_fields = ["messageDate", "receptionDate"]

        for field_name in optional_param_fields:
            if field_name in data and data[field_name] is None:
                del data[field_name]

        return data


@dataclass
class RpcRequest:
    context_id: str
    method: str
    params: Any
    msg_meta: RpcMsgMeta


class JsonRpcRequestAbstractSchema(OrderedCamelCaseSchema):
    jsonrpc = ConstField("2.0")
    context_id = fields.String(required=True, data_key="id")
    method = fields.String(required=True)
    params = fields.Nested(Schema(), required=True)
    msg_meta = fields.Nested(JsonRpcMsgMetaPartial())

    @post_load
    def make_request_object(self, data: dict, **_kwargs) -> RpcRequest:
        if "params" not in data:
            data["params"] = {}
        if "msg_meta" not in data:
            data["msg_meta"] = None
        return RpcRequest(
            data["context_id"], data["method"], data["params"], data["msg_meta"]
        )


@dataclass
class RpcResponse:
    context_id: str
    result: Any
    msg_meta: RpcMsgMeta


class JsonRpcResponseAbstractSchema(OrderedCamelCaseSchema):
    jsonrpc = ConstField("2.0")
    context_id = fields.String(required=True, data_key="id")
    result = fields.Nested(Schema(), required=True)
    msg_meta = fields.Nested(JsonRpcMsgMetaPartial())

    @post_load
    def make_response_object(self, data: dict, **_kwargs) -> RpcResponse:
        if "result" not in data:
            data["result"] = None
        if "msg_meta" not in data:
            data["msg_meta"] = None
        return RpcResponse(data["context_id"], data["result"], data["msg_meta"])


@dataclass
class RpcErrorField:
    code: int
    message: str
    data: dict


class JsonRpcErrorFieldPartial(Schema):
    class Meta:
        ordered = True

    code = fields.Integer()
    message = fields.String()
    data = fields.Dict()

    @post_load
    def make_error_field_object(self, data: dict, **_kwargs) -> RpcErrorField:
        return RpcErrorField(**data)


@dataclass
class RpcErrorResponse:
    context_id: str
    error: RpcErrorField
    msg_meta: RpcMsgMeta


class JsonRpcErrorResponseSchema(OrderedCamelCaseSchema):
    jsonrpc = ConstField("2.0")
    context_id = fields.String(required=True, data_key="id")
    error = fields.Nested(JsonRpcErrorFieldPartial(), required=True)
    msg_meta = fields.Nested(JsonRpcMsgMetaPartial())

    @post_load
    def make_error_response_object(self, data: dict, **_kwargs) -> RpcErrorResponse:
        if "msg_meta" not in data:
            data["msg_meta"] = None
        return RpcErrorResponse(data["context_id"], data["error"], data["msg_meta"])


@dataclass
class RpcNotification:
    method: str
    params: Any


class JsonRpcNotificationAbstractSchema(OrderedCamelCaseSchema):
    jsonrpc = ConstField("2.0")
    method = fields.String(required=True)
    params = fields.Nested(Schema(), required=True)

    @post_load
    def make_notification_object(self, data: dict, **_kwargs) -> RpcNotification:
        return RpcNotification(data["method"], data["params"])
