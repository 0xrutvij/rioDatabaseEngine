import argparse
import utils


def hex_list_dump(file_obj):
    data = file_obj.read().hex()
    return list(map("".join, zip(*[iter(data)]*2)))


def hex_line_and_str_generator(file_obj):
    while next_hex_line := file_obj.read(16):
        hex_list = list(map(lambda chr: format(chr, '02x'), next_hex_line))
        str_list = []
        for hexval in map(lambda x: int(hex(x), 16), next_hex_line):
            if hexval < 0x20 or hexval > 0x7e:
                str_list.append(".")
            else:
                str_list.append(chr(int(hexval)))
        
        yield hex_list, str_list


def hex_dump_main(file):
    print_page_header()
    line_num = 0
    for x, y in hex_line_and_str_generator(file):
        print(f"{line_num:08x}", end="  ")
        if (len_line := len(x)) < 16:
            print(" ".join(x[:8]), " ".join(x[8:]), end="   "*(16-len_line) + " |", sep="  ")    
        else:
            print(" ".join(x[:8]), " ".join(x[8:]), end="  |", sep="  ")
        print("".join(y), end="|\n")
        line_num += 16


def print_page_header():
    print("\nAddress   0  1  2  3  4  5  6  7 "+ " " + " 8  9  A  B  C  D  E  F", end="")
    print("   |0123456789ABCDEF|", end="\n")
    print(utils.SEP_LINE)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OS Agnostic Pythonic Hexdump Util")

    parser.add_argument("file", 
        metavar="<filename>", 
        type=argparse.FileType("rb"), 
        help="The file to be Hex-Dumped")
    
    arg_space = parser.parse_args()
    file = arg_space.file
    # manual testing
    # file = parser.parse_args(["utils.py"]).file
    #chr_list = chr_list_dump(file)
    hex_dump_main(file)