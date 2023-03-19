import json

import pytest
from aws_events.sns import SnsMessage
from aws_events.exceptions import InvalidSubjectException


@pytest.fixture(name="raw_sns_message")
def fixture_raw_sns_message():
    return {
        "SignatureVersion": "1",
        "Timestamp": "1970-01-01T00:00:00.000Z",
        "Signature": "unit_test",
        "SigningCertUrl": "unit_test",
        "MessageId": "1234",
        "Message": json.dumps({"MessageSource": "test", "another_key": "test"}),
        "MessageAttributes": {},
        "Type": "Notification",
        "UnsubscribeUrl": "none",
        "TopicArn": "arn:aws:sns:test",
        "Subject": "TestInvoke",
    }


def test_sns_message_happy_path(raw_sns_message):
    @SnsMessage("TestInvoke")
    def fake_handler(sns_message):
        assert sns_message.message_source == "test"
        assert sns_message.another_key == "test"

    fake_handler(raw_sns_message)


def test_sns_message_raises_on_incorrect_subject(raw_sns_message):
    @SnsMessage("NonexistentSubject")
    def fake_handler(sns_message):
        pass

    with pytest.raises(InvalidSubjectException):
        fake_handler(raw_sns_message)
