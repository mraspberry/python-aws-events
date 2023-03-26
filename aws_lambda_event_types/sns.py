import json
from dataclasses import dataclass, make_dataclass
from typing import Any, Dict, List, Tuple, Optional
from collections.abc import Callable

import inflection
from wrapt import decorator

from .exceptions import InvalidSubjectException


class SnsMessages:
    def __init__(self, subject: str):
        self.subject = subject

    @staticmethod
    def _get_message_fields(message: Dict[str, Any]) -> List[Tuple[str, type]]:
        fields = list()
        for key in message.keys():
            fields.append((inflection.underscore(key), type(message[key])))
        return fields

    @decorator
    def __call__(
        self, wrapped: Callable, _: type, args: tuple, kwargs: Dict
    ) -> Callable:
        sns_messages = _SnsMessages()
        for record in args[0]["Records"]:
            subject = record["Sns"]["Subject"]
            if self.subject != subject:
                raise InvalidSubjectException(
                    f"Message subject '{subject} is not '{self.subject}'"
                )
            message = json.loads(record["Sns"]["Message"])
            fields = self._get_message_fields(message)
            message_class = make_dataclass(
                inflection.camelize(self.subject), fields, eq=True
            )
            sns_messages.add_message(message_class(*message.values()))
        args = list(args)
        args[0] = sns_messages
        return wrapped(*args, **kwargs)


@dataclass
class _SnsMessages:
    messages: Optional[List] = None

    def __iter__(self):
        return iter(self.messages)

    def __len__(self):
        if self.messages is None:
            return 0
        return len(self.messages)

    def __getitem__(self, index: int) -> Any:
        if self.messages is None:
            raise IndexError(f"No message at index {index}")
        return self.messages[index]

    def add_message(self, message: Any):
        if self.messages is None:
            self.messages = list()
        self.messages.append(message)
