def internal_parse_dict_cdata(x):
    parse_dict = {
        "command": "SELECT",
        "column_name_list": ["column_name","data_type", "ordinal_position", "is_nullable","column_key"],
        "table_name": "riobase_columns",
        "condition": {
        "negated": "FALSE",
        "column_name": "table_name",
        "comparator": "=",
        "value": x
        }
    }
    return parse_dict

def internal_parse_dict_tdata(x):
    parse_dict = {
        "command": "SELECT",
        "column_name_list": ["page_size", "record_count"],
        "table_name": "riobase_tables",
        "condition": {
        "negated": "FALSE",
        "column_name": "table_name",
        "comparator": "=",
        "value": x
        }
    }
    return parse_dict
if __name__ == "__main__":
    pass