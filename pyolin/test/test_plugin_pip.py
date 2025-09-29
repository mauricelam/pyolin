import pytest
from pyolin.test.conftest import ErrorWithStderr, assert_contains, assert_startswith, string_block


def test_pip(pyolin):
    with pyolin.popen(
        "pip",
    ) as proc:
        assert proc.stdin and proc.stdout
        proc.stdin.close()
        assert_contains(
            proc.stdout.read().lstrip("\n"),
            "pip <command> [options]",
        )


def test_pip_not_returned(pyolin):
    with pytest.raises(ErrorWithStderr) as exc:
        pyolin("repr(pip)")
    assert_startswith(
        str(exc.value.__cause__),
        "`pip` variable should be returned unmodified in the pyolin the program",
    )
