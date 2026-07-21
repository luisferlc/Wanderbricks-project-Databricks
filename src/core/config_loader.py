import json


def load_config(path):
    with open(path, "r") as f:
        return json.load(f)


def validate_config(config):
    """
    Generic config validation used by Bronze and Silver pipelines.
    """

    required_sections = [
        "source",
        "target",
        "load",
        "tables"
    ]

    for section in required_sections:
        if section not in config:
            raise ValueError(f"Missing {section}")

    if not isinstance(config["tables"], list):
        raise ValueError("Config field 'tables' must be a list")


def validate_table_config(config, required_fields):
    """
    Validate required fields inside every table configuration.

    Example:
        required_fields = [
            "source_table",
            "target_table",
            "key_columns"
        ]
    """

    for index, table in enumerate(config["tables"]):

        for field in required_fields:

            if field not in table:

                raise ValueError(
                    f"Missing field '{field}' "
                    f"in table config at index {index}"
                )


def get_enabled_tables(config):

    return [
        t
        for t in config["tables"]
        if t.get("enabled", True)
    ]