from tabulate import tabulate
import pandas as pd

def table_format_print(output_list, columns):
    df = pd.DataFrame(output_list, columns=columns)
    print(tabulate(df, headers="keys", tablefmt="psql", showindex=False))
    

if __name__ == "__main__":
    output_list = [['asdasda', 10, 'scsc'], ['asdasda', 20, 'scsc'], ['asdasda', 30, 'scsc'], ['asdasda', 40, 'scsc'], ['asdasda', 50, 'scsc'], ['asdasda', 60, 'scsc'], ['asdasda', 70, 'scsc'], ['asdasda', 80, 'scsc'], ['asdasda', 90, 'scsc'], ['asdasda', 100, 'scsc'], ['asdasda', 110, 'scsc'], ['asdasda', 120, 'scsc'], ['asdasda', 130, 'scsc'], ['asdasda', 140, 'scsc'], ['asdasda', 150, 'scsc'], ['asdasda', 160, 'scsc'], ['asdasda', 170, 'scsc']]
    columns =["FirstString", "MiddleInt", "LastString"]
    table_format_print(output_list, columns)