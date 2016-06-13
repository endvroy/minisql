import sqllex
import ply.yacc as yacc
from collections import namedtuple

tokens = sqllex.tokens


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
    p[0] = p[1]['type']
    if p[0] == 'create_index':
        print('is {}'.format(p[0]))
        print(p[1])
    elif p[0] == 'create_table':
        print('is {}'.format(p[0]))
        print(p[1])

def p_insert_statement(p):
    '''
        insert_statement : INSERT INTO ID VALUES LPAREN value_list RPAREN SEMICOLON
    '''
    print('in insert statement')
    table_name = p[3]
    value_list = p[6]


def p_select_statement(p):
    '''
        select_statement : select_all
                        | conditional_select
    '''



def p_delete_statement(p):
    '''
        delete_statement : delete_all
                        | conditional_delete
    '''


def p_drop_statement(p):
    '''
        drop_statement : drop_table
                        | drop_index
    '''


def p_quit_statement(p):
    '''
        quit_statement : QUIT SEMICOLON
    '''
    #todo: quit Minisql



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
    p[0] = p[4] # the column name

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

#Rules for select statement
def p_select_all(p):
    '''
        select_all : SELECT STAR FROM ID SEMICOLON
    '''
    p[0] = p[4]

def p_conditional_select(p):
    '''
        conditional_select : SELECT STAR FROM ID WHERE conditions SEMICOLON
    '''
    dict = {}
    dict['table_name'] = p[4]
    dict['conditions'] = p[6]
    p[0] = dict

def p_conditions(p):
    '''
        conditions : condition
                    | conditions AND condition
                    | conditions OR condition
    '''
    #todo: not complete yet


def p_condition(p):
    '''
        condition :  ID GT value
                    | ID LT value
                    | ID EQ value
                    | ID GE value
                    | ID LE value
                    | ID NE value
    '''
    # todo: not complete yet

#Rules for delete statement

def p_delete_all(p):
    '''
        delete_all : DELETE FROM ID SEMICOLON
    '''
    p[0] = p[3] # the deleted table name

def p_conditional_delete(p):
    '''
        conditional_delete : DELETE FROM ID WHERE conditions SEMICOLON
    '''
    p[0] = (p[3], p[5])

#Rules for drop statement
def p_drop_table(p):
    '''
        drop_table : DROP TABLE ID SEMICOLON
    '''
    p[0] = ('drop_table', p[3])

def p_drop_index(p):
    '''
        drop_index : DROP INDEX ID SEMICOLON
    '''
    p[0] = ('drop_index', p[3])


# Others
def p_error(p):
    print('Oooooooooooooops!!')


parser = yacc.yacc(method='LALR')

if __name__ == '__main__':
    while True:
        try:
            s = input('MiniSQL>  ')
            pass
        except EOFError:
            break
        parser.parse(s)