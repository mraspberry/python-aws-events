import json
from dataclasses import dataclass, make_dataclass
from typing import Any, Dict, List, Tuple
from collections.abc import Callable

import inflection
from wrapt import decorator

from .exceptions import InvalidSubjectException


class SnsMessage:
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
        self, wrapped: Callable, instance: type, args: tuple, kwargs: Dict
    ) -> Callable:
        sns_messages = SnsMessages(list())
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
            sns_messages.messages.append(message_class(*message.values()))
        args = list(args)
        args[0] = sns_messages
        return wrapped(*args, **kwargs)


@dataclass
class SnsMessages:
    messages: list
