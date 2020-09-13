from bs4 import BeautifulSoup
import pandas as pd

debug_mode = 1

with open('portland.osm') as osm_file:
    soup = BeautifulSoup(osm_file, features="html.parser")
    streets_all = soup.find_all('way')
    spisok = []
    i = 0
    for streets in streets_all:
        i+=1
        b = streets.find_all('tag')
        flag = 0
        for tag_line in b:
            a = tag_line['k']
            if 'highway' == a:
                flag = 1
            if 'name' == a and flag == 1 and tag_line['v'][0] == 'B':
                spisok.append(tag_line['v'])

    spisok = pd.Series(spisok)
    spisok.drop_duplicates(inplace=True)
    if debug_mode == 0:
        print(str(len(spisok)))
    else:
        c = ''
        for street in spisok:
            c = c + ' ' + str(street) + ','
        print(c)