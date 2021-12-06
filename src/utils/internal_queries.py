def internal_parse_dict_cdata(table_name):
    parse_dict = {
        "command": "SELECT",
        "column_name_list": [
            "column_name",
            "data_type",
            "ordinal_position", 
            "is_nullable",
            "column_key"
             ],
        "table_name": "riobase_columns",
        "condition": {
        "negated": "FALSE",
        "column_name": "table_name",
        "comparator": "=",
        "value": table_name
        }
    }
    return parse_dict

def internal_parse_dict_tdata(table_name):
    parse_dict = {
        "command": "SELECT",
        "column_name_list": [
            "page_size",
            "record_count"
         ],
        "table_name": "riobase_tables",
        "condition": {
        "negated": "FALSE",
        "column_name": "table_name",
        "comparator": "=",
        "value": table_name
        }
    }
    return parse_dict

def insert_parse_dict_cdata(row_id: int, table_name: str, column_name: str, 
                            data_type: str, ordinal_position: int, is_nullable: str, column_key: str):
    insert_parse_dict = {
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
            row_id,
            table_name, 
            column_name,
            data_type, 
            ordinal_position, 
            is_nullable, 
            column_key
        ]
    }
    return insert_parse_dict

def insert_parse_dict_tdata(row_id, table_name, page_size, record_count):
    insert_parse_dict = {
        "command": "INSERT INTO TABLE",
        "column_name_list": [
            "rowid",
            "table_name",
            "page_size",
            "record_count"
        ],
        "table_name": "riobase_tables",
        "value_list": [
            row_id,
            table_name,
            page_size,
            record_count
        ]
    }
    return insert_parse_dict

def update_tdata_dict(table_name, record_count):
    
    utdict = {
        "command": "UPDATE",
        "table_name": "riobase_tables",
        "operation": {
            "operation_type": "SET",
            "column_name": "record_count",
            "comparator": "=",
            "value": record_count
            },
        "condition": {
            "negated": "FALSE",
            "column_name": "table_name",
            "comparator": "=",
            "value": table_name
        }
    }
        
    return utdict

if __name__ == "__main__":
    pass