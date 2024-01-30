from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class PipeRequest(_message.Message):
    __slots__ = ("new",)
    NEW_FIELD_NUMBER: _ClassVar[int]
    new: _containers.RepeatedScalarFieldContainer[bytes]
    def __init__(self, new: _Optional[_Iterable[bytes]] = ...) -> None: ...

class PipeReply(_message.Message):
    __slots__ = ("ans",)
    ANS_FIELD_NUMBER: _ClassVar[int]
    ans: _containers.RepeatedScalarFieldContainer[bool]
    def __init__(self, ans: _Optional[_Iterable[bool]] = ...) -> None: ...
