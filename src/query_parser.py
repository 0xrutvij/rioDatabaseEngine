import json
from pyparsing import (
    CaselessKeyword,
    CaselessLiteral,
    Group,
    Literal,
    Opt,
    Regex,
    Suppress,
    Word,
    alphanums,
    alphas,
    delimited_list,
    one_of,
    quoted_string,
)

RE_RUN_TESTS = False

def print_result0_as_json(res):
    print(json.dumps(res[0], indent="  "))

def constraint_semantics(constraint_list):

    constraint_set = set(constraint_list)

    constraint_dict = {
        "column_key": "",
        "is_nullable": "YES"
    }

    if "PRIMARY_KEY" in constraint_set:
        constraint_dict["column_key"] = "PRI"
        constraint_dict["is_nullable"] = "NO"
    elif "UNIQUE" in constraint_set:
        constraint_dict["column_key"] = "UNI"
        constraint_dict["is_nullable"] = "NO"

    if "NOT_NULL" in constraint_set:
        constraint_dict["is_nullable"] = "NO"

    return constraint_dict

def split_mapper_ck(string):
    lst = map(str.strip, string.split(","))
    return map(CaselessKeyword, lst)

def column_definition_semantics(def_list):
    name, d_type, prop_dict = def_list
    prop_dict["column_name"] = name
    prop_dict["data_type"] = d_type
    return prop_dict

def table_creation_semantics(parse_list):

    cmd, t_name, = parse_list[0:2]
    col_json_list = parse_list[2:]
    
    columns_list = []
    for i, col_json in enumerate(col_json_list):
        col_json["ordinal_position"] = i+1
        col_json["table_name"] = t_name
        columns_list.append(col_json)

    return {
        "command": cmd,
        "table_name": t_name,
        "column_list": columns_list
    } 

###################################################
################ Special Characters ###############
###################################################
LPAREN, RPAREN, STMT_TERMINATOR = map(Suppress, "();")

###################################################
################ Atomic Definitions ###############
###################################################

# Required Keywords
VERBS = ["select", "create", "drop", "show", "insert", "delete", "update", "set", "exit"]
ADJECTIVES = ["not", "unique", "primary", "key"]
NOUNS = ["tables", "table", "index", "values"]
PREPOSITIONS = ["from", "into", "where"]


SELECT, CREATE, DROP, SHOW, INSERT, DELETE = split_mapper_ck("SELECT, CREATE, DROP, SHOW, INSERT, DELETE")
UPDATE, NOT, UNIQUE, PRIMARY, KEY, TABLES = split_mapper_ck("UPDATE, NOT, UNIQUE, PRIMARY, KEY, TABLES")
TABLE, INDEX, VALUES, FROM, INTO, WHERE, SET = split_mapper_ck("TABLE, INDEX, VALUES, FROM, INTO, WHERE, SET")
NULL, TINYINT, SMALLINT, INT, BIGINT, LONG = split_mapper_ck("NULL, TINYINT, SMALLINT, INT, BIGINT, LONG")
FLOAT, DOUBLE, YEAR, TIME, DATETIME, DATE, TEXT = split_mapper_ck("FLOAT, DOUBLE, YEAR, TIME, DATETIME, DATE, TEXT")

# Keywords
data_type = (NULL | TINYINT | SMALLINT | INT | BIGINT | LONG | FLOAT | DOUBLE | YEAR
             | TIME | DATETIME | DATE | TEXT)

data_type.set_name("data type")

keyword = (SELECT | CREATE | DROP | SHOW | INSERT | DELETE | UPDATE | NOT | UNIQUE | PRIMARY 
            | KEY | TABLES | TABLE | INDEX | VALUES | FROM | INTO | WHERE | SET | data_type)

keyword.set_name("keyword")

# Reserved Identifiers
db_tables, db_columns = map(CaselessLiteral, ("davisbase_tables", "davisbase_columns"))
reserved_identifiers = db_tables | db_columns
reserved_identifiers.set_name("reserved identfier")

# Identifiers
identifier =  ~keyword + Word(alphas, alphanums + "_")
identifier.set_name("identifier")

###################################################
###################### DDL ########################
###################################################

#---------------- SHOW TABLES ------------------#

show_table_stmt = SHOW + TABLES + STMT_TERMINATOR

#---------------- TABLE CREATION ------------------#

NOT_NULL = Group(NOT + NULL).set_parse_action(lambda: "NOT_NULL")
PRIMARY_KEY = Group(PRIMARY + KEY).set_parse_action(lambda: "PRIMARY_KEY")

column_contstraint_opts = UNIQUE | PRIMARY_KEY | NOT_NULL

column_contstraints = column_contstraint_opts * (0, 3)

# input for parse action ["const1", "const2" ...] or []
column_contstraints.set_parse_action(constraint_semantics)

new_col_name = ~reserved_identifiers + identifier

column_definition = new_col_name + data_type + column_contstraints

# input for parse action [name, data_type, {dict_constraints}]
column_definition.set_parse_action(column_definition_semantics)

multi_column_definitions = LPAREN + delimited_list(column_definition) + RPAREN

new_table_name = new_col_name.copy()

create_table_stmt = (Group(CREATE + TABLE).set_parse_action(lambda ls: " ".join(ls[0]))
                     + new_table_name 
                     + multi_column_definitions 
                     + STMT_TERMINATOR
                    )

#input for parse action ["command", "table_name", [{col1 deets}, {col2 deets}]]
create_table_stmt.set_parse_action(table_creation_semantics)


#---------------- TABLE DROP ------------------#

drop_table_stmt = (Group(DROP + TABLE).set_parse_action(lambda ls: " ".join(ls[0]))
                     + identifier
                     + STMT_TERMINATOR
                    )

drop_table_stmt.set_parse_action(lambda plist: {
    "command": plist[0],
    "table_name": plist[1],
})


#---------------- INDEX CREATION ------------------#

# CREATE INDEX table_name (column_name);

create_index_stmt = (Group(CREATE + INDEX).set_parse_action(lambda ls: " ".join(ls[0]))
                     + identifier
                     + LPAREN + identifier + RPAREN
                     + STMT_TERMINATOR
                    )

create_index_stmt.set_parse_action(lambda plist: {
    "command": plist[0],
    "table_name": plist[1],
    "column_name": plist[2]
})

###################################################
###################### DML ########################
###################################################

integer = Regex(r"[+-]?\d+")
integer.set_name("int")

numeric_literal = Regex(r"[+-]?\d*\.?\d+([eE][+-]?\d+)?")
numeric_literal.set_name("real number")

string_literal = quoted_string.set_parse_action(lambda toks: str.replace(toks[0], "'", "").replace('"', ""))
string_literal.set_name("string")

# Null is an empty string <- should take no memory in record body
NULL.set_parse_action(lambda: "")

literal_value = (
    numeric_literal
    | string_literal
    | NULL
)

literal_value.set_name("value literal")

#comment = "--" + rest_of_line

#---------------- ROW INSERTION ------------------#

#INSERT INTO TABLE (column_list) table_name VALUES (value1,value2,value3, ...);

value_list = Suppress(VALUES) + LPAREN + delimited_list(literal_value) + RPAREN
value_list.set_name("list of column values")

column_list = LPAREN + delimited_list(identifier) + RPAREN
column_list.set_name("paranthesized column name list")

insert_row_stmt = (Group(INSERT + INTO + TABLE).set_parse_action(lambda ls: " ".join(ls[0]))
                   + Opt(column_list).set_parse_action(lambda x: [x])
                   + identifier
                   + value_list.set_parse_action(lambda x: [x])
                   + STMT_TERMINATOR)

insert_row_stmt.set_parse_action(lambda plist: {
    "command": plist[0],
    "column_name_list": plist[1].as_list(),
    "table_name": plist[2],
    "value_list": plist[3].as_list()
})

#---------------- DELETE RECORD ------------------#

#Operators

bin_op = one_of([">", "<", ">=", "<=", "=", "<>"]).set_name("comparator")

predicate = (Opt(NOT).set_parse_action(lambda x: "TRUE" if x else "FALSE")
             + identifier 
             + bin_op 
             + literal_value)


predicate.set_parse_action(lambda plist: {
    "negated": plist[0],
    "column_name": plist[1],
    "comparator": plist[2],
    "value": plist[3]
})

where_clause = Suppress(WHERE) + predicate

# DELETE FROM TABLE table_name [WHERE condition];
delete_record_stmt = (DELETE + Suppress(FROM + TABLE) 
                     + identifier + where_clause 
                     + STMT_TERMINATOR)

delete_record_stmt.set_parse_action(lambda plist: {
    "command": plist[0],
    "table_name": plist[1],
    "condition": plist[2]
})

#---------------- UPDATE RECORD ------------------#
# UPDATE table_name SET column_name = value WHERE condition;

set_clause = SET + identifier + Literal("=") + literal_value

set_clause.set_parse_action(lambda plist: {
        "operation_type": plist[0],
        "column_name": plist[1],
        "comparator": plist[2],
        "value": plist[3]
})

update_record_stmt = UPDATE + identifier + set_clause + where_clause + STMT_TERMINATOR

update_record_stmt.set_parse_action(lambda plist: {
    "command": plist[0],
    "table_name": plist[1],
    "operation": plist[2],
    "condition": plist[3]
})

###################################################
###################### DQL ########################
###################################################

#---------------- SIMPLE SELECT ------------------#

select_clause = (SELECT 
                + Group(Literal("*") | delimited_list(identifier))
                )


select_statement = (select_clause 
                    + Suppress(FROM)
                    + identifier
                    + Opt(where_clause).set_parse_action(lambda plist: plist or {})
                    + STMT_TERMINATOR)

select_statement.set_parse_action(lambda plist: {
    "command": plist[0],
    "column_name_list": [] if "*" in (x := plist[1].as_list()) else x,
    "table_name": plist[2],
    "condition": plist[3]
})

###########################################################
###################### SQL Statement ######################
###########################################################

show_table_stmt.set_parse_action(
    lambda: select_statement.parse_string("SELECT table_name FROM riobase_tables;")
)

statement = (create_table_stmt | drop_table_stmt | create_index_stmt | insert_row_stmt
             | delete_record_stmt | update_record_stmt | select_statement | show_table_stmt)


###########################################################
###########################################################


if RE_RUN_TESTS:
    new_col_name.run_tests(
        """
        #valid ident
        table_name

        #clash with reserved ident
        davisbase_tables

        #valid ident
        davisbase_table

        #is a data_type
        int

        #is a keyword
        select

        #valid ident
        a_long_name_with_num43r5
        """
    )

    x = value_list.parse_string("VALUES (123e10,'Company','123 Corp','-1')")
    print(x)

    r = column_list.parse_string("(organisation_id, name, organisation_type, parent_organisation_id)")
    print(r)


    print(predicate.parse_string("c_name > '12'"))

    print(predicate.parse_string("NOT c_name > '12'"))

    print(set_clause.parse_string("SET ContactName='Juan'"))