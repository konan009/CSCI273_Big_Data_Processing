#!/usr/bin/python
# Mapper
# mugaddan@ateneo.edu
import sys
import re

book_names = [    
    "Genesis","Exodus","Leviticus","Numbers","Deuteronomy",
    "Joshua","Judges","Ruth","1-Samuel","2-Samuel","1-Kings",
    "2-Kings","1-Chronicles","2-Chronicles","Ezra","Nehemiah",
    "Esther","Job","Psalms","Proverbs","Ecclesiastes",
    "Song-of-Solomon","Isaiah","Jeremiah","Lamentations",
    "Ezekiel","Daniel","Hosea","Joel","Amos","Obadiah",
    "Jonah","Micah","Nahum","Habakkuk","Zephaniah","Haggai",
    "Zechariah","Malachi","Matthew","Mark","Luke","John",
    "Acts","Romans","1-Corinthians","2-Corinthians","Galatians",
    "Ephesians","Philippians","Colossians","1-Thessalonians",
    "2-Thessalonians","1-Timothy","2-Timothy","Titus","Philemon",
    "Hebrews","James","1-Peter","2-Peter","1-John","2-John",
    "3-John","Jude","Revelation"
]

# Regex that match 00:00:00 format ( BookNo:ChapterNo:VerseNo )
regex_for_book = r"""[0-9]+:[0-9]+:[0-9]+"""
book_count = dict()
empty_line_counter = 0

current_book = None
for line in sys.stdin:
    line = line.strip()
    # Use this variable to get the current book number
    line_info = re.findall(regex_for_book, line)

    # Sample token split from the example \ Sample from the example code
    wordcount = len(line.strip().split())
    if len(line_info) > 0 :
        info = line_info[0]
        info = info.split(":")
        current_book = info[0]

        if info[0] not in book_count:
            book_count[current_book] = 0
        
        book_count[current_book] += wordcount
        empty_line_counter = 0
    elif current_book is not None and wordcount > 0:
        book_count[current_book] += wordcount
    elif wordcount == 0:
        empty_line_counter += 1

    # If accumulated no. new line is greater than or equal three `current_book`
    # pointer should be null
    if empty_line_counter >= 3:
        current_book = None
        
for key,value in list(book_count.items()):
    print( "%s\t%d" % (book_names[int(key)-1], value) )
