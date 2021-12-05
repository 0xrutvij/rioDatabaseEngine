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

def insert_parse_dict_cdata(table_name, column_name, data_type, ordinal_position, is_nullable, column_key):
    insert_parse_dict = {
        "command": "INSERT INTO TABLE",
        "column_name_list": [
            "table_name",
            "column_name",
            "data_type",
            "ordinal_position", 
            "is_nullable",
            "column_key"
        ],
        "table_name": "riobase_columns",
        "value_list": [
            table_name, 
            column_name,
            data_type, 
            ordinal_position, 
            is_nullable, 
            column_key
        ]
    }
    return insert_parse_dict

def insert_parse_dict_tdata(table_name, page_size, record_count):
    insert_parse_dict = {
        "command": "INSERT INTO TABLE",
        "column_name_list": [
            "table_name",
            "page_size",
            "record_count"
        ],
        "table_name": "riobase_tables",
        "value_list": [
            table_name,
            page_size,
            record_count
        ]
    }
    return insert_parse_dict


if __name__ == "__main__":
    pass