"""Built-in mutation-operator library + registry loader (task.bench.operators).

Every issue-#749 operator family ships one positive operator and the
hard-negative twin set carries era-conditional gold (design §5). Deployments
register domain operators through the YAML ``module`` + ``class`` +
``params`` loader in :mod:`registry` without touching core.
"""

from okg.bench.injection.operators.library import (
    DEFAULT_V1_OPERATOR_TYPES,
    ClaimValueOperator,
)
from okg.bench.injection.operators.registry import (
    load_operators,
    load_operators_yaml,
)

__all__ = [
    "DEFAULT_V1_OPERATOR_TYPES",
    "ClaimValueOperator",
    "load_operators",
    "load_operators_yaml",
]
