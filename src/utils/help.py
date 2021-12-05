def help():
    help = ["clear  : Clear previous input data",
            "insert : Insert data into a particular table",
            "create : create a new table in the database"
            "select : Query data from the database",
            "update : Update data in the tables",
            "delete : Delete data from a table",
            "drop   : Delete table from database",
            "exit   : Exit davisbase RioDB"  
    ]
    return '\n'.join(help)

if __name__ == "__main__":
    print(help())