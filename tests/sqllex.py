import ply.lex as lex

reserved = (
    'SELECT', 'CREATE', 'INSERT', 'DELETE', 'DROP', 'TABLE', 'PRIMARY', 'KEY',
    'UNIQUE', 'INT', 'CHAR', 'FLOAT', 'ON', 'FROM', 'QUIT', 'VALUES', 'INTO',
    'INDEX', 'WHERE', 'AND', 'OR',
)

tokens = reserved + \
         (
             # Identifier, including names for table, column, index..
             'ID',

             # Delimeters
             'COMMA', 'LPAREN', 'RPAREN', 'SEMICOLON',

             # Operation
             'LT', 'GT', 'LE', 'GE', 'EQ', 'NE',

             # Literal
             'ICONST', 'SCONST', 'FCONST',

             # Symbol
             'STAR',

         )

t_ignore = ' \t\x0c'


def t_error(t):
    print('Oooooooooooop Error!')
    t.lexer.skip(1)

#Symbols
t_STAR = '\*'


# Delimeters
t_COMMA = ','
t_LPAREN = '\('
t_RPAREN = '\)'
t_SEMICOLON = ';'

# Operators
t_LT = r'<'
t_GT = r'>'
t_LE = r'<='
t_GE = r'>='
t_EQ = r'=='
t_NE = r'!='


# Integer Literal
t_ICONST = r'\d+([uU]|[lL]|[uU][lL]|[lL][uU])?'

# Floating literal
t_FCONST = r'((\d+)(\.\d+)(e(\+|-)?(\d+))? | (\d+)e(\+|-)?(\d+))([lL]|[fF])?'

# String literal
t_SCONST = r'\'([^\\\n]|(\\.))*?\''

reserved_map = {}

for r in reserved:
    reserved_map[r.lower()] = r


def t_ID(t):
    r'[A-Za-z_][\w_]*'
    t.type = reserved_map.get(t.value.lower(), "ID")
    return t


lexer = lex.lex()

if __name__ == '__main__':
    while True:
        data = input('MiniSQL>')
        lexer.input(data)
        while True:
            tok = lexer.token()
            if not tok:
                break
            print(tok, tok.value, tok.type, tok.value)
