from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class NextRequest(_message.Message):
    __slots__ = ("context_json",)
    CONTEXT_JSON_FIELD_NUMBER: _ClassVar[int]
    context_json: bytes
    def __init__(self, context_json: _Optional[bytes] = ...) -> None: ...

class NextReply(_message.Message):
    __slots__ = ("response_json",)
    RESPONSE_JSON_FIELD_NUMBER: _ClassVar[int]
    response_json: bytes
    def __init__(self, response_json: _Optional[bytes] = ...) -> None: ...
