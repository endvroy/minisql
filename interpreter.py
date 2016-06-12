
import ply.lex as lex
import ply.yacc as yacc
import re
from math import *
class node:

    def __init__(self, data):
        self._data = data
        self._children = []

    def getdata(self):
        return self._data

    def getchildren(self):
        return self._children

    def add(self, node):
        self._children.append(node)

    def print_node(self, prefix):
        print '  '*prefix,'+',self._data
        for child in self._children:
            child.print_node(prefix+1)

#TOKENS
tokens=('SELECT','FROM','WHERE','NAME','AND','OR','COMMA',
'LP','RP','ON','PRIMARY','KEY','UNIQUE','CREATE','INSERT','INTO','VALUES','DELETE','DROP','INDEX','TABLE',
'INT','FLOAT','CHAR','NUMBER','SEMICOLON','QUIT')

literals = ['=','+','-','*','>','<' ]
#DEFINE OF TOKENS
def t_LP(t):
    r'\('
    return t

def t_RP(t):
    r'\)'
    return t

def t_SELECT(t):
    r'SELECT | select'
    return t

def t_DELETE(t):
    r'DELETE | delete'
    return t

def t_QUIT(t):
    r'QUIT | quit'
    return t

def t_FROM(t):
    r'FROM | from'
    return t

def t_WHERE(t):
    r'WHERE | where'
    return t

def t_OR(t):
    r'OR | or'
    return t

def t_AND(t):
    r'AND | and'
    return t

def t_CREATE(t):
    r'CREATE | create'
    return t

def t_DROP(t):
    r'DROP | drop'
    return t

def t_INSERT(t):
    r'INSERT | insert'
    return t

def t_INTO(t):
    r'INTO | into'
    return t

def t_INDEX(t):
    r'INDEX | index'
    return t

def t_ON(t):
    r'ON | on'
    return t

def t_UNIQUE(t):
    r'UNIQUE | unique'
    return t

def t_PRIMARY(t):
    r'PRIMARY | primary'
    return t

def t_KEY(t):
    r'KEY | key'
    return t

def t_VALUES(t):
    r'VALUES | values'
    return t

def t_TABLE(t):
    r'TABLE'
    return t

def t_COMMA(t):
    r','
    return t

def t_SEMICOLON(t):
    r';'
    return t

def t_INT(t):
    r'INT | int'
    return t

def t_FLOAT(t):
    r'FLOAT | float'
    return t

def t_CHAR(t):
    r'CHAR | char'
    return t

def t_NUMBER(t):
    r'[0-9]+'
    return t

def t_NAME(t):
    r'[A-Za-z]+|[a-zA-Z_][a-zA-Z0-9_]*|[A-Z]*\.[A-Z]$'
    return t

# IGNORED
t_ignore = " \t"
def t_error(t):
    print("Illegal character '%s'" % t.value[0])
    t.lexer.skip(1)

# LEX ANALYSIS
lex.lex()

#PARSING



def p_delete(t):
    '''SQL :  DELETE FROM table SEMICOLON
            | DELETE FROM table WHERE lst SEMICOLON
                '''
    if len(t)==4:
        t[0]=node('[DELETE FROM]')
        t[0].add(t[3])
    else:
        t[0]=node('[DELETE FROM]')
        t[0].add(t[3])
        t[0].add(node('[WHERE]'))
        t[0].add(t[5])



def p_drop(t):
    '''SQL :   DROP TABLE table SEMICOLON
                | DROP INDEX NAME SEMICOLON
                '''
    t[0]=node('[DROP]')
    if isinstance(t[3],node):
        t[0].add(node('[TABLE]'))
        t[0].add(t[3])
    else:
        t[0].add(node('[INDEX]'))
        t[0].add(node(t[3]))


def p_quit(t):
    '''SQL :   QUIT SEMICOLON
               '''
    exit()

def p_select(t):
    '''SQL :   SELECT list FROM table WHERE lst SEMICOLON
                | SELECT list FROM table SEMICOLON
                '''
    if len(t)==8:
        t[0]=node('[SELECT]')
        t[0].add(t[2])
        t[0].add(node('[FROM]'))
        t[0].add(t[4])
        t[0].add(node('[WHERE]'))
        t[0].add(t[6])
    else:
        t[0]=node('[SELECT]')
        t[0].add(t[2])
        t[0].add(node('[FROM]'))
        t[0].add(t[4])

def p_table(t):
    '''table :   NAME
               '''
    t[0]=node('[TABLE]')
    t[0].add(node(t[1]))

def p_lst(t):
    ''' lst  :   condition
               | condition AND condition
               | condition OR condition
              '''
    if len(t)==2:
        t[0]=node('[CONDITION]')
        t[0].add(t[1])
    elif t[2]=='AND':
        t[0]=node('[CONDITIONS]')
        t[0].add(t[1])
        t[0].add(node('[AND]'))
        t[0].add(t[3])
    elif t[2]=='OR':
        t[0]=node('[CONDITIONS]')
        t[0].add(t[1])
        t[0].add(node('[OR]'))
        t[0].add(t[3])


def p_condition(t):
    ''' condition :   NAME '>' NUMBER
                    | NAME '<' NUMBER
                    | NAME '=' NUMBER
                    | NAME '>' NAME
                    | NAME '<' NAME
                    | NAME '=' NAME
                  '''
    t[0]=node('[TERM]')
    t[0].add(node(str(t[1])))
    t[0].add(node(t[2]))
    t[0].add(node(str(t[3])))



def p_list(t):
    ''' list : '*'
            '''
    t[0]=node('[FIELD]')
    t[0].add(node(t[1]))


def p_error(t):
    print("Syntax error at '%s'" % t.value)

yacc.yacc()

while 1:
    try:
        s = raw_input('Please enter: ')
        if not s.endswith(';'):
            print "please add semicolon at the end of SQL! "
            exit()
        pass
    except EOFError:
        break
    parse=yacc.parse(s)
    parse.print_node(0)
