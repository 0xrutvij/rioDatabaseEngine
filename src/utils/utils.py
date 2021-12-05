from settings import Settings
from string import Template
import sys
import os

SEP_LINE = Settings.line("-", 78)
DB_NAME = "RioDBLite"
WELC_MSG = f"Welcome to {DB_NAME}"
VER_MSG = Template("$db_name Version $ver")
HELP_MSG = "\nType \"help;\" to display supported commands."

def splash_screen():
    print(SEP_LINE)
    print(WELC_MSG)
    print(VER_MSG.substitute(db_name=DB_NAME, ver=Settings.get_version()))
    print(Settings.get_copyright())
    print(HELP_MSG)
    print(SEP_LINE)

# Disable
def blockPrint():
    sys.stdout = open(os.devnull, 'w')

# Restore
def enablePrint():
    sys.stdout = sys.__stdout__

if __name__ == "__main__":
    splash_screen()