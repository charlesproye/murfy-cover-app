import numpy as np
import pytest

from core.numpy_utils import numpy_safe_eval


def test_numpy_safe_eval():
    assert numpy_safe_eval("1 + 1") == 2
    assert numpy_safe_eval("np.log(10)") == np.log(10)
    assert numpy_safe_eval("np.log(10)", x=10) == np.log(10)

    assert numpy_safe_eval("np.log(x) + np.log(y)", x=10, y=20) == np.log(10) + np.log(
        20
    )

    assert (
        numpy_safe_eval("np.sqrt(x)", x=np.array([9, 16, 25])) == np.array([3, 4, 5])
    ).all()

    assert (
        round(
            numpy_safe_eval("1.022 + -0.0142 * np.log1p(x/12.536)", x=1e5),
            2,
        )
        == 0.89
    )


def test_numpy_safe_eval_forbidden():
    with pytest.raises(SyntaxError):
        numpy_safe_eval("import pandas")

    with pytest.raises(NameError):
        numpy_safe_eval("__import__('os').system('echo UNSAFE')")

    with pytest.raises(NameError):
        numpy_safe_eval("max(10)")
