import pandas as pd
import numpy as np

output_list = [['asdasda', 10, 'scsc'], ['asdasda', 20, 'scsc'], ['asdasda', 30, 'scsc'], ['asdasda', 40, 'scsc'], ['asdasda', 50, 'scsc'], ['asdasda', 60, 'scsc'], ['asdasda', 70, 'scsc'], ['asdasda', 80, 'scsc'], ['asdasda', 90, 'scsc'], ['asdasda', 100, 'scsc'], ['asdasda', 110, 'scsc'], ['asdasda', 120, 'scsc'], ['asdasda', 130, 'scsc'], ['asdasda', 140, 'scsc'], ['asdasda', 150, 'scsc'], ['asdasda', 160, 'scsc'], ['asdasda', 170, 'scsc']]

def table(output_list):
    df = pd.DataFrame(output_list, columns =["FirstString", "MiddleInt", "LastString"]) 
    return df

print(table(output_list))