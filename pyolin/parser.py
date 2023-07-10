import ast
import io
import textwrap
import token
import tokenize
import traceback
from types import CodeType
from typing import Any, Dict, Iterable, List, Sequence, Tuple

from .util import debug, NoMoreRecords


def _split_last_statement(tokens: Iterable[tokenize.TokenInfo], prog: str) -> Tuple[int, int]:
    def _line_pos_to_pos(linepos):
        line, pos = linepos
        offset = 0
        for _ in range(1, line):
            offset = prog.index('\n', offset) + 1
        return pos + offset

    started = False
    for tok in reversed(list(tokens)):
        if tok.exact_type in (token.SEMI, token.NEWLINE):
            if started:
                return _line_pos_to_pos(tok.start), _line_pos_to_pos(tok.end)
        elif tok.type not in (token.ENDMARKER, token.COMMENT, token.NL):
            started = True
    return 0, 0


def _replace_double_semicolons(tokens: Iterable[tokenize.TokenInfo]) -> Iterable[tokenize.TokenInfo]:
    double_semis = []
    last_semi = False
    tokens = list(tokens)
    for i, tok in enumerate(tokens):
        if tok.string == ';':
            if last_semi:
                double_semis.append(i-1)
            else:
                last_semi = True
        else:
            last_semi = False
    for i in reversed(double_semis):
        del tokens[i + 1]
        _replace_with_newline(tokens, i)
    debug('tokens', list(tokens), double_semis)
    return tokens


def _replace_with_newline(tokens: List[tokenize.TokenInfo], pos: int) -> None:
    tok = tokens[pos]
    tokens[pos] = tokenize.TokenInfo(
        token.NEWLINE,
        '\n',
        tok.start,
        tok.end,
        tok.line)
    line_offset = 0
    for i, tok2 in enumerate(tokens[pos + 1:]):
        if i == 0:
            line_offset = tok2.start[1]
        if tok2.start[0] != tok.start[0]:
            line_offset = 0
        tokens[i + pos + 1] = tokenize.TokenInfo(
            tok2.type, tok2.string,
            start=(tok2.start[0] + 1, tok2.start[1] - line_offset),
            end=(tok2.end[0] + 1, tok2.end[1] - line_offset),
            line=tok2.line)


def _parse(prog: str) -> Tuple[ast.AST, ast.AST]:
    '''
    Parse the given pyolin program into the exec statements and eval statements that can be evaluated
    directly, applying the necessary syntax transformations as necessary.
    '''
    prog_io = io.StringIO(prog)
    tokens = tokenize.generate_tokens(prog_io.readline)
    tokens = _replace_double_semicolons(tokens)
    prog = tokenize.untokenize(tokens)
    debug(f'newprog=\n{prog}')
    split_start, split_end = _split_last_statement(tokens, prog)
    prog_stmts = prog[:split_start]
    prog_expr = prog[split_end:]
    debug(f'stmt={prog_stmts} expr={prog_expr}')
    try:
        exec_statements = ast.parse(prog_stmts, mode='exec')
    except SyntaxError as e:
        raise RuntimeError(textwrap.dedent(
            f'''\
            Invalid syntax:
              {prog_stmts}
              {" "*(e.offset-1)}^'''))
    try:
        # Try to parse as generator expression (the common case)
        eval_expr = ast.parse(f'({prog_expr})', mode='eval')
    except SyntaxError as e:
        # Try to parse as <expr> if <condition>
        try:
            eval_expr = ast.parse(f'({prog_expr} else _UNDEFINED_)', mode='eval')
        except SyntaxError:
            try:
                ast.parse(f'{prog_expr}', mode='exec')
            except SyntaxError:
                raise RuntimeError(textwrap.dedent(
                    f'''\
                    Invalid syntax:
                      {prog_expr}
                      {" "*(e.offset-2)}^'''))
            else:
                raise RuntimeError(textwrap.dedent(f'''\
                    Cannot evaluate value from statement:
                      {prog_expr}'''))
    debug(ast.dump(eval_expr))
    return exec_statements, eval_expr


class UserError(RuntimeError):

    def formatted_tb(self):
        cause = self.__cause__
        assert isinstance(cause, BaseException)
        return traceback.format_exception(
            cause.__class__,
            cause,
            cause.__traceback__.tb_next)  # pylint: disable=no-member

    def __str__(self):
        return ''.join(self.formatted_tb()).rstrip('\n')


class Prog:

    _exec: CodeType
    _eval: CodeType

    def __init__(self, prog: Any):
        if hasattr(prog, '__code__'):
            self._exec = compile('', filename='pyolin_user_prog.py', mode='exec')
            self._eval = prog.__code__
        else:
            exec_code, eval_code = _parse(prog)
            debug('Resulting AST', ast.dump(exec_code), ast.dump(eval_code))
            self._exec = compile(exec_code, filename='pyolin_user_prog.py', mode='exec')
            self._eval = compile(eval_code, filename='pyolin_user_prog.py', mode='eval')

    def exec(self, globals: Dict[str, Any]) -> Any:
        try:
            exec(self._exec, globals)
            return eval(self._eval, globals)
        except NoMoreRecords:
            raise
        except Exception as e:
            raise UserError() from e
