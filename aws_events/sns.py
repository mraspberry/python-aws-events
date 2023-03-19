import json
from dataclasses import make_dataclass
from typing import Any
from collections.abc import Callable

import inflection
from wrapt import decorator

from .exceptions import InvalidSubjectException


class SnsMessage:
    def __init__(self, subject: str):
        self.subject = subject

    @staticmethod
    def _get_message_fields(message: dict[str, Any]) -> list[tuple[str, type]]:
        fields = list()
        for key in message.keys():
            fields.append((inflection.underscore(key), type(message[key])))
        return fields

    @decorator
    def __call__(
        self, wrapped: Callable, instance: type, args: tuple, kwargs: dict
    ) -> Callable:
        subject = args[0]["Subject"]
        if self.subject != subject:
            raise InvalidSubjectException(
                f"Message subject '{subject} is not '{self.subject}'"
            )
        message = json.loads(args[0]["Message"])
        fields = self._get_message_fields(message)
        message_class = make_dataclass(
            inflection.camelize(self.subject), fields, eq=True
        )
        sns_message = message_class(*message.values())
        args = list(args)
        args[0] = sns_message
        return wrapped(*args, **kwargs)
