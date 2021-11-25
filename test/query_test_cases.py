######################################

create_table_test_case = """
    CREATE TABLE new_table (
        column_name1 int NOT NULL UNIQUE, 
        column_name2 year PRIMARY KEY
    );"""

create_table_test_result = """{
  "command": "CREATE TABLE",
  "table_name": "new_table",
  "column_list": [
    {
      "column_key": "UNI",
      "is_nullable": "NO",
      "column_name": "column_name1",
      "data_type": "INT",
      "ordinal_position": 1,
      "table_name": "new_table"
    },
    {
      "column_key": "PRI",
      "is_nullable": "NO",
      "column_name": "column_name2",
      "data_type": "YEAR",
      "ordinal_position": 2,
      "table_name": "new_table"
    }
  ]
}"""

######################################

create_index_test_case = "CREATE INDEX table_name (column_name);"

create_index_test_result = """{
  "command": "CREATE INDEX",
  "table_name": "table_name",
  "column_name": "column_name"
}"""

######################################

drop_table_test_case = "DROP TABLE table_name;"

drop_table_test_result = """{
  "command": "DROP TABLE",
  "table_name": "table_name"
}"""

######################################

insert_row_test_case = """INSERT INTO 
  TABLE (organisation_id, name, organisation_type, parent_organisation_id) 
  organisation 
  VALUES ('123','Company','123 Corp','-1');"""

insert_row_test_result = """{
  "command": "INSERT INTO TABLE",
  "column_name_list": [
    "organisation_id",
    "name",
    "organisation_type",
    "parent_organisation_id"
  ],
  "table_name": "organisation",
  "value_list": [
    "'123'",
    "'Company'",
    "'123 Corp'",
    "'-1'"
  ]
}"""

######################################

######################################

######################################

######################################


######################################
create_default_table_test_case = """
    CREATE TABLE new_table (
        rowid            INT PRIMARY KEY,
        table_name       TEXT NOT NULL,
        column_name      TEXT NOT NULL UNIQUE,
        data_type        TEXT NOT NULL,
        ordinal_position TINYINT UNIQUE,
        is_nullable      TEXT
    );"""

create_default_table_test_result = """{
  "command": "CREATE TABLE",
  "table_name": "new_table",
  "column_list": [
    {
      "column_key": "PRI",
      "is_nullable": "NO",
      "column_name": "rowid",
      "data_type": "INT",
      "ordinal_position": 1,
      "table_name": "new_table"
    },
    {
      "column_key": "",
      "is_nullable": "NO",
      "column_name": "table_name",
      "data_type": "TEXT",
      "ordinal_position": 2,
      "table_name": "new_table"
    },
    {
      "column_key": "UNI",
      "is_nullable": "NO",
      "column_name": "column_name",
      "data_type": "TEXT",
      "ordinal_position": 3,
      "table_name": "new_table"
    },
    {
      "column_key": "",
      "is_nullable": "NO",
      "column_name": "data_type",
      "data_type": "TEXT",
      "ordinal_position": 4,
      "table_name": "new_table"
    },
    {
      "column_key": "UNI",
      "is_nullable": "NO",
      "column_name": "ordinal_position",
      "data_type": "TINYINT",
      "ordinal_position": 5,
      "table_name": "new_table"
    },
    {
      "column_key": "",
      "is_nullable": "YES",
      "column_name": "is_nullable",
      "data_type": "TEXT",
      "ordinal_position": 6,
      "table_name": "new_table"
    }
  ]
}"""
