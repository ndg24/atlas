from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class NLRequest(_message.Message):
    __slots__ = ("question", "dataset", "schema_json")
    QUESTION_FIELD_NUMBER: _ClassVar[int]
    DATASET_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_JSON_FIELD_NUMBER: _ClassVar[int]
    question: str
    dataset: str
    schema_json: str
    def __init__(self, question: _Optional[str] = ..., dataset: _Optional[str] = ..., schema_json: _Optional[str] = ...) -> None: ...

class NLResponse(_message.Message):
    __slots__ = ("plan_json", "raw_llm_output", "error")
    PLAN_JSON_FIELD_NUMBER: _ClassVar[int]
    RAW_LLM_OUTPUT_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    plan_json: str
    raw_llm_output: str
    error: str
    def __init__(self, plan_json: _Optional[str] = ..., raw_llm_output: _Optional[str] = ..., error: _Optional[str] = ...) -> None: ...

class ExplainRequest(_message.Message):
    __slots__ = ("question", "result_arrow_ipc")
    QUESTION_FIELD_NUMBER: _ClassVar[int]
    RESULT_ARROW_IPC_FIELD_NUMBER: _ClassVar[int]
    question: str
    result_arrow_ipc: bytes
    def __init__(self, question: _Optional[str] = ..., result_arrow_ipc: _Optional[bytes] = ...) -> None: ...

class ExplainResponse(_message.Message):
    __slots__ = ("explanation",)
    EXPLANATION_FIELD_NUMBER: _ClassVar[int]
    explanation: str
    def __init__(self, explanation: _Optional[str] = ...) -> None: ...

class NarrateFindingsRequest(_message.Message):
    __slots__ = ("findings_json",)
    FINDINGS_JSON_FIELD_NUMBER: _ClassVar[int]
    findings_json: str
    def __init__(self, findings_json: _Optional[str] = ...) -> None: ...

class NarrateFindingsResponse(_message.Message):
    __slots__ = ("narrative",)
    NARRATIVE_FIELD_NUMBER: _ClassVar[int]
    narrative: str
    def __init__(self, narrative: _Optional[str] = ...) -> None: ...

class SuggestQuestionsRequest(_message.Message):
    __slots__ = ("schema_json", "summary_json")
    SCHEMA_JSON_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_JSON_FIELD_NUMBER: _ClassVar[int]
    schema_json: str
    summary_json: str
    def __init__(self, schema_json: _Optional[str] = ..., summary_json: _Optional[str] = ...) -> None: ...

class SuggestQuestionsResponse(_message.Message):
    __slots__ = ("questions",)
    QUESTIONS_FIELD_NUMBER: _ClassVar[int]
    questions: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, questions: _Optional[_Iterable[str]] = ...) -> None: ...
