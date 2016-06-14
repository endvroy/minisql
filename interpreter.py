import ply.lex as lex
import ply.yacc as yacc

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
t_EQ = r'='
t_NE = r'!='


# Integer Literal
def t_ICONST(t):
    r'\d+([uU]|[lL]|[uU][lL]|[lL][uU])?'
    t.value = int(t.value)
    return t

# Floating literal
def t_FCONST(t):
    r'((\d+)(\.\d+)(e(\+|-)?(\d+))? | (\d+)e(\+|-)?(\d+))([lL]|[fF])?'
    t.value = float(t.value)
    return t

# String literal
t_SCONST = r'\'([^\\\n]|(\\.))*?\''

reserved_map = {}

for r in reserved:
    reserved_map[r.lower()] = r


def t_ID(t):
    r'[A-Za-z_][\w_]*'
    t.type = reserved_map.get(t.value.lower(), "ID")
    return t

def t_error(t):
    print('Syntax Error at {}'.format(t.value))
    t.lexer.skip(1)


lexer = lex.lex()




# translation unit

def p_sql_statement(p):
    '''
        sql_statement : create_statement
                        | insert_statement
                        | select_statement
                        | delete_statement
                        | drop_statement
                        | quit_statement
    '''
    print('sql statement')


def p_create_statement(p):
    '''
        create_statement : create_table
                          | create_index
    '''
    type_code = p[1]['type']
    if type_code == 'create_index':
        print('is {}'.format(p[0]))
        print(p[1])
        # todo: call the api to create index
    elif type_code == 'create_table':
        print('is {}'.format(p[0]))
        print(p[1])
        # todo: call the api to create table


def p_insert_statement(p):
    '''
        insert_statement : INSERT INTO ID VALUES LPAREN value_list RPAREN SEMICOLON
    '''
    print('in insert statement')
    table_name = p[3]
    value_list = p[6]
    print('table name is {} and value list is {}'.format(table_name, value_list))
    # todo : call the api to insert record

def p_select_statement(p):
    '''
        select_statement : select_all
                        | conditional_select
    '''
    type_code = p[1]['type']
    if type_code == 'select_all':
        print('in select all')
        print(p[1])
        # todo : call the api to select all
    elif type_code == 'conditional_select':
        print('in conditional select')
        print(p[1])
        # todo : call the api to select with conditions



def p_delete_statement(p):
    '''
        delete_statement : delete_all
                        | conditional_delete
    '''
    type_code = p[1]['type']
    if type_code == 'delete_all':
        print('in delete_all')
        print(p[1])
        # todo : call the api to delete all records of the table
    elif type_code == 'conditional_delete':
        print('in conditional delete')
        print(p[1])
        # todo : call the api to delete with conditions

def p_drop_statement(p):
    '''
        drop_statement : drop_table
                        | drop_index
    '''
    type_code = p[1]['type']
    if type_code == 'drop_table':
        print('in drop table')
        print(p[1])
        # todo : call the api to drop the specified table
    elif type_code == 'drop_index':
        print('in drop index')
        print(p[1])
        # todo: call the api to drop the specified index


def p_quit_statement(p):
    '''
        quit_statement : QUIT SEMICOLON
    '''
    print('in quit')
    # todo: quit Minisql


# Rules for create statement
def p_create_table(p):
    '''
        create_table : CREATE TABLE ID LPAREN column_list RPAREN SEMICOLON
                    | CREATE TABLE ID LPAREN column_list COMMA primary_clause RPAREN SEMICOLON
    '''
    print('create table')
    dict = {}
    dict['type'] = 'create_table'
    dict['table_name'] = p[3]
    dict['element_list'] = p[5]
    if len(p) == 8:
        dict['primary'] = False
    elif len(p) == 10:
        dict['primary'] = True
        dict['primary key'] = p[7]
    p[0] = dict


def p_create_index(p):
    '''
        create_index : CREATE INDEX ID ON ID LPAREN ID RPAREN SEMICOLON
    '''
    print('create index')
    dict = {}
    dict['type'] = 'create_index'
    dict['index_name'] = p[3]
    dict['table_name'] = p[5]
    dict['column_name'] = p[7]
    p[0] = dict


def p_column_list(p):
    '''
        column_list : column
                    | column_list COMMA column
    '''
    p[0] = []
    if len(p) == 2:
        p[0].append(p[1])
    elif len(p) == 4:
        p[0] += p[1]
        p[0].append(p[3])


def p_column(p):
    '''
        column :  ID column_type
                | ID column_type UNIQUE
    '''
    p[0] = (p[1], p[2], False)
    if len(p) == 4:
        p[0][2] = True


def p_column_type(p):
    '''
        column_type : INT
                    | FLOAT
                    | CHAR LPAREN ICONST RPAREN
    '''
    type_code = p[1].lower()
    if type_code == 'int':
        p[0] = ('int', 1)
    elif type_code == 'float':
        p[0] = ('float', 1)
    elif type_code == 'char':
        p[0] = ('char', p[3])


def p_primary_clause(p):
    '''
        primary_clause : PRIMARY KEY LPAREN ID RPAREN
    '''
    p[0] = p[4]  # the column name


# Rules for insert statement
def p_value_list(p):
    '''
        value_list : value
                    | value_list COMMA value
    '''
    p[0] = []
    if len(p) == 2:
        p[0].append(p[1])
    elif len(p) == 4:
        p[0] += p[1]
        p[0].append(p[3])


def p_value(p):
    '''
        value : ICONST
                | FCONST
                | SCONST
    '''
    p[0] = p[1]


# Rules for select statement
def p_select_all(p):
    '''
        select_all : SELECT STAR FROM ID SEMICOLON
    '''
    dict = {}
    dict['type'] = 'select_all'
    dict['table_name'] = p[4]
    p[0] = dict


def p_conditional_select(p):
    '''
        conditional_select : SELECT STAR FROM ID WHERE conditions SEMICOLON
    '''
    dict = {}
    dict['type'] = 'conditional_select'
    dict['table_name'] = p[4]
    dict['conditions'] = p[6]
    p[0] = dict


def p_conditions(p):
    '''
        conditions : condition
                    | conditions AND condition
                    | conditions OR condition
    '''
    p[0] = []       # [condition, AND/OR, condition, AND/OR.....]
    if len(p) == 2:
        p[0].append(p[1])
    elif len(p) == 4:
        p[0] += p[1]
        p[0].append(p[2])
        p[0].append(p[3])

def p_condition(p):
    '''
        condition :  ID GT value
                    | ID LT value
                    | ID EQ value
                    | ID GE value
                    | ID LE value
                    | ID NE value
    '''
    p[0] = (p[1], p[2], p[3])


# Rules for delete statement

def p_delete_all(p):
    '''
        delete_all : DELETE FROM ID SEMICOLON
    '''
    dict = {}
    dict['type'] = 'delete_all'
    dict['table_id'] = p[3]
    p[0] = dict


def p_conditional_delete(p):
    '''
        conditional_delete : DELETE FROM ID WHERE conditions SEMICOLON
    '''
    dict = {}
    dict['type'] = 'conditional_delete'
    dict['table_name'] = p[3]
    dict['conditions'] = p[5]
    p[0] = dict



# Rules for drop statement
def p_drop_table(p):
    '''
        drop_table : DROP TABLE ID SEMICOLON
    '''
    dict = {}
    dict['type'] = 'drop_table'
    dict['table_name'] = p[3]
    p[0] = dict

def p_drop_index(p):
    '''
        drop_index : DROP INDEX ID SEMICOLON
    '''
    dict = {}
    dict['type'] = 'drop_index'
    dict['index_name'] = p[3]
    p[0] = dict


# Others
def p_error(p):
    print('Syntax error!')


parser = yacc.yacc(method='LALR')

if __name__ == '__main__':
    while True:
        try:
            s = input('MiniSQL>  ')
        except EOFError:
            break
        parser.parse(s)
