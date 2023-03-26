import json

import pytest
from aws_lambda_event_types.sns import SnsMessages
from aws_lambda_event_types.exceptions import InvalidSubjectException


@pytest.fixture(name="raw_sns_messages")
def fixture_raw_sns_message():
    return {
        "Records": [
            {
                "EventSource": "aws:sns",
                "EventVersion": "1.0",
                "EventSubscriptionArn": "test",
                "Sns": {
                    "Type": "Notification",
                    "MessageId": "baed22fa-3171-5c5f-8dbb-7e6b1db53b37",
                    "TopicArn": "test",
                    "Subject": "TestInvoke",
                    "Message": '{"MessageSource": "test", "anotherKey": "test"}',
                    "Timestamp": "2023-03-19T15:30:12.047Z",
                    "SignatureVersion": "1",
                    "Signature": "euFKILuJJlz9uAtfu1gDO4ztmRmgwww/OviRFZico/ks1XYY/KaU0YzmOo51oUAI7SwtIW7S/L87HT6WgFtwag6y/p3Fd9jY4rn5gDL7C883BlcdU14UJaLHic5tIqZ24iQzI2ZcQAXkk9E5sjaY9jDqgfPTbSN91H4AMvqrEbtASKxVcdMK29Ck91Xjceyj3xdTz1aTQ7iWl6sSimtRZ5UWMDopNl26Q8mXCTvIKtwoKr4T35BVrQB2mtyhZ00srXudhf5cemTsEyG5YuAb6/TU58GxQuWvuoS6VzSXEcxaqAxKubCxhnCTI0cKXf/blIekeJbk+pURC/vPgQbXZg==",
                    "SigningCertUrl": "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-56e67fcb41f6fec09b0196692625d385.pem",
                    "UnsubscribeUrl": "https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-1:217108017677:aws-service-event-topic:f05f1a1b-d45c-4fc8-af99-f6f3eae0e251",
                    "MessageAttributes": {},
                },
            }
        ]
    }


def test_sns_messages_has_snake_names(raw_sns_messages):
    @SnsMessages("TestInvoke")
    def fake_handler(sns_messages):
        assert sns_messages[0].message_source == "test"
        assert sns_messages[0].another_key == "test"

    fake_handler(raw_sns_messages)


def test_sns_messages_iteration(raw_sns_messages):
    @SnsMessages("TestInvoke")
    def fake_handler(sns_messages):
        iterated = False
        for message in sns_messages:
            iterated = True
            assert message.message_source == "test"
            assert message.another_key == "test"
        assert iterated

    fake_handler(raw_sns_messages)


def test_sns_messages_len(raw_sns_messages):
    @SnsMessages("TestInvoke")
    def fake_handler(sns_messages):
        assert len(sns_messages) == 1

    fake_handler(raw_sns_messages)


def test_sns_messages_raises_on_incorrect_subject(raw_sns_messages):
    @SnsMessages("NonexistentSubject")
    def fake_handler(sns_message):
        pass

    with pytest.raises(InvalidSubjectException):
        fake_handler(raw_sns_messages)
