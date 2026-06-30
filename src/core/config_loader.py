import json


def load_config(path):
    with open(path, "r") as f:
        return json.load(f)


def validate_config(config):
    required_sections = ["source", "target", "load", "tables"]

    for section in required_sections:
        if section not in config:
            raise ValueError(f"Missing {section}")


def get_enabled_tables(config):
    return [
        t for t in config["tables"]
        if t.get("enabled", True)
    ]