class AwsEventException(Exception):
    """Base Exception class"""

    pass


class InvalidSubjectException(AwsEventException):
    pass
