from ingest.stream.schemas import added_required, subject_for

BRANCH = {
    "type": "object",
    "required": ["header"],
    "allOf": [{"if": {}, "then": {"properties": {"body": {"required": ["train_id"]}}}}],
}


def with_branch_required(fields):
    return {**BRANCH, "allOf": [{"if": {}, "then": {"properties": {"body": {"required": fields}}}}]}


def test_required_added_inside_a_conditional_is_reported():
    new = with_branch_required(["train_id", "platform"])
    assert added_required(BRANCH, new) == ["#/allOf/0/then/properties/body: ['platform']"]


def test_removing_or_keeping_required_fields_is_fine():
    assert added_required(BRANCH, BRANCH) == []
    assert added_required(BRANCH, with_branch_required([])) == []


def test_a_new_required_list_counts_as_added_in_full():
    new = {**BRANCH, "properties": {"header": {"required": ["msg_type"]}}}
    assert added_required(BRANCH, new) == ["#/properties/header: ['msg_type']"]


def test_topic_contracts_use_the_topic_name_strategy():
    topics = {"raw.trust.movement"}
    assert subject_for("raw.trust.movement", topics) == "raw.trust.movement-value"
    assert subject_for("silver.trust_movement", topics) == "silver.trust_movement"
