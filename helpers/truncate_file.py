# -*- coding: utf-8 -*-
# @Author: Eduardo Santos
# @Date:   2023-02-26 22:05:07
# @Last Modified by:   Eduardo Santos
# @Last Modified time: 2023-02-26 22:17:57

def main():
    with open("../conditions.csv", "a") as file:
        file.truncate(20000000) # truncate to 20 MB

if __name__ == "__main__":
    main()
