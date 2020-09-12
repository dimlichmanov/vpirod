from bs4 import BeautifulSoup

with open('portland.osm') as osm_file:
    soup = BeautifulSoup(osm_file, features="html.parser")
    streets = soup.find('way')
    b = streets.find_all('tag')
    print(b)
    spisok = []
    flag = 0
    for tag_line in b:
        a = tag_line['k']
        if 'highway' == a:
            flag = 1
        if 'maxspeed' == a and flag == 1:
            spisok.append(tag_line['v'])
    print(spisok)
