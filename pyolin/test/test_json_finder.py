from pyolin.plugins.json import JsonFinder


def test_simple():
    assert JsonFinder().add_input(r'{"a": 1, "b": 2}') == [{"a": 1, "b": 2}]


def test_nested():
    assert JsonFinder().add_input(r'{"a": 1, "b": {"c": [1, 2, 3]}}') == [
        {
            "a": 1,
            "b": {"c": [1, 2, 3]},
        }
    ]


def test_partial():
    finder = JsonFinder()
    assert finder.add_input('{"a":') == []
    assert finder.add_input("1}") == [{"a": 1}]


def test_multiple():
    assert JsonFinder().add_input(r'{"a": 1, "b": 2}{"a": 2, "b": 3}') == [
        {"a": 1, "b": 2},
        {"a": 2, "b": 3},
    ]


def test_string_with_brackets():
    finder = JsonFinder()
    assert finder.add_input(r'{"a": "}"') == []
    assert finder.add_input("}") == [{"a": "}"}]


def test_escape_quotes():
    assert JsonFinder().add_input(r'{"a": "This quote is escaped: \" :}"}') == [
        {"a": 'This quote is escaped: " :}'}
    ]


def test_trailing_whitespace():
    assert JsonFinder().add_input('{"a": 1} \t \r\n') == [{"a": 1}]
