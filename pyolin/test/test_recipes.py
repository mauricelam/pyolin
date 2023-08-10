"""Recipes that are useful as examples in addition to testing."""


def test_struct_pack(pyolin):
    """Parse a C-struct (or equivalent) representation using the struct module.

    https://docs.python.org/3/library/struct.html.

    Example from https://stackoverflow.com/a/15597001/2921519"""

    input_bytes = (
        b"\x35\x49\xC9\x5C\x00\x00\x00\x00\x38\x27\x0B\x00"
        b"\x00\x00\x00\x00\x04\x00\x04\x00\x5A\x00\x07\x00"
        b"\x35\x49\xC9\x5C\x00\x00\x00\x00\x38\x27\x0B\x00"
        b"\x00\x00\x00\x00\x01\x00\x50\x00\x01\x00\x00\x00"
        b"\x35\x49\xC9\x5C\x00\x00\x00\x00\x38\x27\x0B\x00"
        b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
    )
    # struct input_event {
    #   struct timeval time {
    #     time_t tv_sec
    #     suseconds_t tv_usec
    #   }
    #   unsigned short type;
    #   unsigned short code;
    #   unsigned int value;
    # };

    assert pyolin(
        "cfg.header = ('tv_sec', 'tv_usec', 'type', 'code', 'value');"
        "struct.iter_unpack('llHHI', file.bytes)",
        input_=input_bytes,
    ) == (
        """\
        | tv_sec     | tv_usec | type | code | value  |
        | ---------- | ------- | ---- | ---- | ------ |
        | 1556695349 | 730936  | 4    | 4    | 458842 |
        | 1556695349 | 730936  | 1    | 80   | 1      |
        | 1556695349 | 730936  | 0    | 0    | 0      |
        """
    )

def test_base64(pyolin):
    """Base64 encode a given string from stdin"""
    assert pyolin("base64.b64encode(contents.bytes)", input_=b'Hello world') == (
        """\
        SGVsbG8gd29ybGQ=
        """
    )
