from typing import Any

import numpy as np
from numpy.typing import NDArray


def numpy_safe_eval(expression: str, **params) -> NDArray[Any]:
    """
    Evaluate a mathematical expression safely, ensuring that numpy is imported before evaluation.

    This function uses a restricted "sandbox" for evaluation, allowing only access
    to the numpy library (as 'np') and the parameters you provide. It blocks
    access to all other built-in functions to prevent unsafe code execution.

    Args:
        expression: The mathematical expression to evaluate, e.g., "x + np.log(y)".
        **params: Keyword arguments representing variables in the expression, e.g., x=1, y=2.

    Returns:
        The result of the evaluated expression.

    Raises:
        NameError: If the expression tries to use a variable or function not provided.
        Exception: Other exceptions from numpy or math errors (e.g., division by zero).
    """
    # Create a safe dictionary of allowed global functions.
    # We explicitly block __builtins__ and only allow numpy.
    safe_globals = {"__builtins__": {}, "np": np}

    # The 'params' dictionary serves as the local scope, containing variables like x and y.
    return eval(expression, safe_globals, params)
