import ast
import io
import textwrap
import token
import tokenize
import traceback

from .util import debug, NoMoreRecords


def _split_last_statement(tokens, prog):
    def _line_pos_to_pos(linepos):
        line, pos = linepos
        offset = 0
        for i in range(1, line):
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


def _parse(prog):
    '''
    Parse the given pol program into the exec statements and eval statements that can be evaluated
    directly, applying the necessary syntax transformations as necessary.
    '''
    prog_io = io.StringIO(prog)
    tokens = tokenize.generate_tokens(prog_io.readline)
    split_start, split_end = _split_last_statement(tokens, prog)
    prog_stmts = prog[:split_start]
    prog_expr = prog[split_end:]
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
        return traceback.format_exception(
            self.__cause__,
            self.__cause__,
            self.__cause__.__traceback__.tb_next)

    def __str__(self):
        return ''.join(self.formatted_tb()).rstrip('\n')


class Prog:
    def __init__(self, prog):
        self._exec, self._eval = _parse(prog)
        debug('Resulting AST', ast.dump(self._exec), ast.dump(self._eval))
        self._exec = compile(self._exec, filename='pol_user_prog.py', mode='exec')
        self._eval = compile(self._eval, filename='pol_user_prog.py', mode='eval')

    def exec(self, globals):
        try:
            exec(self._exec, globals)
            return eval(self._eval, globals)
        except NoMoreRecords:
            raise
        except Exception as e:
            raise UserError() from e
