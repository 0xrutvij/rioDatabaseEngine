"""
Creates the two metadata tables for the database using the internal meta 
language which is json like in format.
"""
from settings import Settings

riobase_columns_cdata= {
            "column_names": ["rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable", "column_key"],
            "data_types": ["INT", "TEXT", "TEXT", "TEXT", "TINYINT", "TEXT", "TEXT"],
            "nullability": ["NO", "NO", "NO", "NO", "NO", "NO", "YES"],
            "column_keys": ["UNI", "", "", "", "", "", "", ""]
        }

riobase_tables_cdata = {
            "column_names": ["rowid", "table_name", "page_size", "record_count"],
            "data_types": ["INT", "TEXT", "SMALLINT", "INT"],
            "nullability": ["NO", "NO", "NO", "NO"],
            "column_keys": ["UNI", "UNI", "", ""]
        }

create_riobase_tables = {
  "command": "CREATE TABLE",
  "table_name": "riobase_tables",
  "column_list": [
    {
        "column_name": "rowid",
        "column_key": "UNI",
        "is_nullable": "NO",
        "data_type": "INT",
        "ordinal_position": 1,
        "table_name": "riobase_tables"
    },
    {
        "column_name": "table_name",
        "column_key": "UNI",
        "is_nullable": "NO",
        "data_type": "TEXT",
        "ordinal_position": 2,
        "table_name": "riobase_tables"
    },
    {
        "column_name": "page_size",
        "column_key": "",
        "is_nullable": "NO",
        "data_type": "SMALLINT",
        "ordinal_position": 3,
        "table_name": "riobase_tables"
    },
    {
        "column_name": "record_count",
        "column_key": "",
        "is_nullable": "NO",
        "data_type": "INT",
        "ordinal_position": 4,
        "table_name": "riobase_tables"
    },
  ]
}

create_riobase_columns = {
  "command": "CREATE TABLE",
  "table_name": "riobase_columns",
  "column_list": [
    {
        "column_name": "rowid",
        "column_key": "UNI",
        "is_nullable": "NO",
        "data_type": "INT",
        "ordinal_position": 1,
        "table_name": "riobase_columns"
    },
    {
        "column_name": "table_name",
        "column_key": "",
        "is_nullable": "NO",
        "data_type": "TEXT",
        "ordinal_position": 2,
        "table_name": "riobase_columns"
    },
    {
        "column_name": "column_name",
        "column_key": "",
        "is_nullable": "NO",
        "data_type": "TEXT",
        "ordinal_position": 3,
        "table_name": "riobase_columns"
    },
    {
        "column_name": "data_type",
        "column_key": "",
        "is_nullable": "NO",
        "data_type": "TEXT",
        "ordinal_position": 4,
        "table_name": "riobase_columns"
    },
    {
        "column_name": "ordinal_position",
        "column_key": "",
        "is_nullable": "NO",
        "data_type": "TINYINT",
        "ordinal_position": 5,
        "table_name": "riobase_columns"
    },
    {
        "column_name": "is_nullable",
        "column_key": "",
        "is_nullable": "NO",
        "data_type": "TEXT",
        "ordinal_position": 6,
        "table_name": "riobase_columns"
    },
    {
        "column_name": "column_key",
        "column_key": "",
        "is_nullable": "YES",
        "data_type": "TEXT",
        "ordinal_position": 7,
        "table_name": "riobase_columns"
    },
  ]
}

fill_riobase_tables = [
    {
        "command": "INSERT INTO TABLE",
        "column_name_list": [
            "rowid",
            "table_name",
            "page_size",
            "record_count"
            ],
        "table_name": "riobase_tables",
        "value_list": [
            1,
            "riobase_tables",
            Settings.get_page_size(),
            2
            ]
    },
    {
        "command": "INSERT INTO TABLE",
        "column_name_list": [
            "rowid",
            "table_name",
            "page_size",
            "record_count"
            ],
        "table_name": "riobase_tables",
        "value_list": [
            2,
            "riobase_columns",
            Settings.get_page_size(),
            11
            ]
    },
]

fill_riobase_columns = [
    {
        "command": "INSERT INTO TABLE",
        "column_name_list": [
            "rowid",
            "table_name",
            "column_name",
            "data_type",
            "ordinal_position",
            "is_nullable",
            "column_key"
            ],
        "table_name": "riobase_columns",
        "value_list": [
            1,
            "riobase_tables",
            "rowid",
            "INT",
            1,
            "NO",
            "UNI"
            ]
    },
    {
        "command": "INSERT INTO TABLE",
        "column_name_list": [
            "rowid",
            "table_name",
            "column_name",
            "data_type",
            "ordinal_position",
            "is_nullable",
            "column_key"
            ],
        "table_name": "riobase_columns",
        "value_list": [
            2,
            "riobase_tables",
            "table_name",
            "TEXT",
            2,
            "NO",
            "UNI"
            ]
    },
    {
        "command": "INSERT INTO TABLE",
        "column_name_list": [
            "rowid",
            "table_name",
            "column_name",
            "data_type",
            "ordinal_position",
            "is_nullable",
            "column_key"
            ],
        "table_name": "riobase_columns",
        "value_list": [
            3,
            "riobase_tables",
            "page_size",
            "SMALLINT",
            3,
            "NO",
            ""
            ]
    },
    {
        "command": "INSERT INTO TABLE",
        "column_name_list": [
            "rowid",
            "table_name",
            "column_name",
            "data_type",
            "ordinal_position",
            "is_nullable",
            "column_key"
            ],
        "table_name": "riobase_columns",
        "value_list": [
            4,
            "riobase_tables",
            "record_count",
            "INT",
            4,
            "NO",
            ""
            ]
    },
    {
        "command": "INSERT INTO TABLE",
        "column_name_list": [
            "rowid",
            "table_name",
            "column_name",
            "data_type",
            "ordinal_position",
            "is_nullable",
            "column_key"
            ],
        "table_name": "riobase_columns",
        "value_list": [
            5,
            "riobase_columns",
            "rowid",
            "INT",
            1,
            "NO",
            "UNI"
            ]
    },
    {
        "command": "INSERT INTO TABLE",
        "column_name_list": [
            "rowid",
            "table_name",
            "column_name",
            "data_type",
            "ordinal_position",
            "is_nullable",
            "column_key"
            ],
        "table_name": "riobase_columns",
        "value_list": [
            6,
            "riobase_columns",
            "table_name",
            "TEXT",
            2,
            "NO",
            ""
            ]
    },
    {
        "command": "INSERT INTO TABLE",
        "column_name_list": [
            "rowid",
            "table_name",
            "column_name",
            "data_type",
            "ordinal_position",
            "is_nullable",
            "column_key"
            ],
        "table_name": "riobase_columns",
        "value_list": [
            7,
            "riobase_columns",
            "column_name",
            "TEXT",
            3,
            "NO",
            ""
            ]
    },
    {
        "command": "INSERT INTO TABLE",
        "column_name_list": [
            "rowid",
            "table_name",
            "column_name",
            "data_type",
            "ordinal_position",
            "is_nullable",
            "column_key"
            ],
        "table_name": "riobase_columns",
        "value_list": [
            8,
            "riobase_columns",
            "data_type",
            "TEXT",
            4,
            "NO",
            ""
            ]
    },
    {
        "command": "INSERT INTO TABLE",
        "column_name_list": [
            "rowid",
            "table_name",
            "column_name",
            "data_type",
            "ordinal_position",
            "is_nullable",
            "column_key"
            ],
        "table_name": "riobase_columns",
        "value_list": [
            9,
            "riobase_columns",
            "ordinal_position",
            "TINYINT",
            5,
            "NO",
            ""
            ]
    },
    {
        "command": "INSERT INTO TABLE",
        "column_name_list": [
            "rowid",
            "table_name",
            "column_name",
            "data_type",
            "ordinal_position",
            "is_nullable",
            "column_key"
            ],
        "table_name": "riobase_columns",
        "value_list": [
            10,
            "riobase_columns",
            "is_nullable",
            "TEXT",
            6,
            "NO",
            ""
            ]
    },
    {
        "command": "INSERT INTO TABLE",
        "column_name_list": [
            "rowid",
            "table_name",
            "column_name",
            "data_type",
            "ordinal_position",
            "is_nullable",
            "column_key"
            ],
        "table_name": "riobase_columns",
        "value_list": [
            11,
            "riobase_columns",
            "column_key",
            "TEXT",
            7,
            "YES",
            ""
            ]
    },
]

