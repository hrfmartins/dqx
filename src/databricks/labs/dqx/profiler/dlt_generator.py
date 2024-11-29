import json
import re
import logging

from databricks.labs.dqx.base import DQEngineBase
from databricks.labs.dqx.profiler.common import val_to_str
from databricks.labs.dqx.profiler.profiler import DQProfile

__name_sanitize_re__ = re.compile(r"[^a-zA-Z0-9]+")
logger = logging.getLogger(__name__)


class DQDltGenerator(DQEngineBase):

    def generate_dlt_rules(
        self, rules: list[DQProfile], action: str | None = None, language: str = "SQL"
    ) -> list[str] | str:
        """
        Generates Delta Live Table (DLT) rules in the specified language.

        :param rules: A list of data quality profiles to generate rules for.
        :param action: The action to take on rule violation (e.g., "drop", "fail").
        :param language: The language to generate the rules in ("SQL" or "Python").
        :return: A list of strings representing the DLT rules in SQL, or a string representing the DLT rules in Python.
        :raises ValueError: If the specified language is not supported.
        """

        lang = language.lower()

        if lang == "sql":
            return self._generate_dlt_rules_sql(rules, action)

        if lang == "python":
            return self._generate_dlt_rules_python(rules, action)

        raise ValueError(f"Unsupported language '{language}'. Only 'SQL' and 'Python' are supported.")

    @staticmethod
    def _dlt_generate_is_in(col_name: str, **params: dict):
        """
        Generates a Delta Live Table (DLT) rule to check if a column's value is in a specified list.

        :param col_name: The name of the column to check.
        :param params: Additional parameters, including the list of values to check against.
        :return: A string representing the DLT rule.
        """
        in_str = ", ".join([val_to_str(v) for v in params["in"]])
        return f"{col_name} in ({in_str})"

    @staticmethod
    def _dlt_generate_min_max(col_name: str, **params: dict):
        """
        Generates a Delta Live Table (DLT) rule to check if a column's value is within a specified range.

        :param col_name: The name of the column to check.
        :param params: Additional parameters, including the minimum and maximum values.
        :return: A string representing the DLT rule.
        """
        min_limit = params.get("min")
        max_limit = params.get("max")
        if min_limit is not None and max_limit is not None:
            # We can generate `col between(min, max)`,
            # but this one is easier to modify if you need to remove some of the bounds
            return f"{col_name} >= {val_to_str(min_limit)} and {col_name} <= {val_to_str(max_limit)}"

        if max_limit is not None:
            return f"{col_name} <= {val_to_str(max_limit)}"

        if min_limit is not None:
            return f"{col_name} >= {val_to_str(min_limit)}"

        return ""

    @staticmethod
    def _dlt_generate_is_not_null_or_empty(col_name: str, **params: dict):
        """
        Generates a Delta Live Table (DLT) rule to check if a column's value is not null or empty.

        :param col_name: The name of the column to check.
        :param params: Additional parameters, including whether to trim strings.
        :return: A string representing the DLT rule.
        """
        trim_strings = params.get("trim_strings", True)
        msg = f"{col_name} is not null and "
        if trim_strings:
            msg += "trim("
        msg += col_name
        if trim_strings:
            msg += ")"
        msg += " <> ''"
        return msg

    _checks_mapping = {
        "is_not_null": lambda col_name, **params: f"{col_name} is not null",
        "is_in": _dlt_generate_is_in,
        "min_max": _dlt_generate_min_max,
        "is_not_null_or_empty": _dlt_generate_is_not_null_or_empty,
    }

    def _generate_dlt_rules_python(self, rules: list[DQProfile], action: str | None = None) -> str:
        """
        Generates Delta Live Table (DLT) rules in Python.

        :param rules: A list of data quality profiles to generate rules for.
        :param action: The action to take on rule violation (e.g., "drop", "fail").
        :return: A string representing the DLT rules in Python.
        """
        if rules is None or len(rules) == 0:
            return ""

        expectations = {}
        for rule in rules:
            rule_name = rule.name
            col_name = rule.column
            params = rule.parameters or {}
            function_mapping = self._checks_mapping
            if rule_name not in function_mapping:
                logger.info(f"No rule '{rule_name}' for column '{col_name}'. skipping...")
                continue
            expr = function_mapping[rule_name](col_name, **params)
            if expr == "":
                logger.info("Empty expression was generated for rule '{nm}' for column '{cl}'")
                continue
            exp_name = re.sub(__name_sanitize_re__, "_", f"{col_name}_{rule_name}")
            expectations[exp_name] = expr

        if len(expectations) == 0:
            return ""

        json_expectations = json.dumps(expectations)
        expectations_mapping = {
            "drop": "@dlt.expect_all_or_drop",
            "fail": "@dlt.expect_all_or_fail",
            None: "@dlt.expect_all",
        }
        decorator = expectations_mapping.get(action, "@dlt.expect_all")

        return f"""{decorator}(
{json_expectations}
)"""

    def _generate_dlt_rules_sql(self, rules: list[DQProfile], action: str | None = None) -> list[str]:
        """
        Generates Delta Live Table (DLT) rules in sql.

        :param rules: A list of data quality profiles to generate rules for.
        :param action: The action to take on rule violation (e.g., "drop", "fail").
        :return: A list of DLT rules.
        :raises ValueError: If the specified language is not supported.
        """
        if rules is None or len(rules) == 0:
            return []

        dlt_rules = []
        act_str = ""
        if action == "drop":
            act_str = " ON VIOLATION DROP ROW"
        elif action == "fail":
            act_str = " ON VIOLATION FAIL UPDATE"
        for rule in rules:
            rule_name = rule.name
            col_name = rule.column
            params = rule.parameters or {}
            function_mapping = self._checks_mapping
            if rule_name not in function_mapping:
                logger.info(f"No rule '{rule_name}' for column '{col_name}'. skipping...")
                continue
            expr = function_mapping[rule_name](col_name, **params)
            if expr == "":
                logger.info("Empty expression was generated for rule '{nm}' for column '{cl}'")
                continue
            # TODO: generate constraint name in lower_case, etc.
            dlt_rule = f"CONSTRAINT {col_name}_{rule_name} EXPECT ({expr}){act_str}"
            dlt_rules.append(dlt_rule)

        return dlt_rules
