import json

import pytest

from ingest.stomp_bridge.frames import FrameError, message_key, split_frame

MOVEMENT = {"header": {"msg_type": "0003"}, "body": {"train_id": "872L09MX03"}}
ACTIVATION = {"header": {"msg_type": "0001"}, "body": {"train_id": "705Y66MY03"}}


def test_a_frame_is_split_into_its_messages_in_order():
    assert split_frame(json.dumps([MOVEMENT, ACTIVATION])) == [MOVEMENT, ACTIVATION]


def test_empty_array_is_a_legal_frame():
    assert split_frame("[]") == []


def test_single_object_is_treated_as_a_one_message_frame():
    assert split_frame(json.dumps(MOVEMENT)) == [MOVEMENT]


@pytest.mark.parametrize("body", ["not json", '"a string"', "[1, 2]", "[{}, 3]"])
def test_malformed_frames_raise_frame_error(body):
    with pytest.raises(FrameError):
        split_frame(body)


def test_message_key_is_the_train_id():
    assert message_key(MOVEMENT) == "872L09MX03"


@pytest.mark.parametrize(
    "message", [{"header": {}}, {"body": {}}, {"body": "x"}, {"body": {"train_id": ""}}]
)
def test_message_key_is_none_when_there_is_no_train_id(message):
    assert message_key(message) is None
