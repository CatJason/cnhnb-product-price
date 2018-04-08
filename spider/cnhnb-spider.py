import urllib
from bs4 import BeautifulSoup
import sys
import re
import MySQLdb
import string
import time



first_urls = []
products = []
second_urls = []
count = 0
datas = []
updatas = []
day = time.strftime("%Y%m%d")

def get_content(url):
    """ doc."""
    html = urllib.urlopen(url)
    content = html.read()
    html.close
    return content


def get_data(info,id):
    data = []
    soup = BeautifulSoup(info, "lxml")
    try:
        name_zero = soup.find('div', attrs={"class": "tit clearfix"})
        if name_zero != None:
            name_zero = name_zero.find('strong')
            name = name_zero.get_text().replace('\t', '').replace('\n', '').replace(' ', '').encode('utf-8')

            if soup.find('div', attrs={"class": "txt clearfix mt10"}).label != None:
                soup.find('div', attrs={"class": "txt clearfix mt10"}).label.extract()
            time_zero = soup.find('div', attrs={"class": "txt clearfix mt10"}).find('span', class_="fl")
            if time_zero != None:
                time = time_zero.get_text().replace('\t', '').replace('\n', '').replace(' ', '').encode('utf-8')
                time = time[0 :10] + " " + time[10:20]
            adress_zero = soup.find('div', attrs={"class": "txt clearfix"}).find('span')
            if adress_zero.find("label") != None:
                adress_zero.find('label').extract()
            adress = adress_zero.get_text().replace('\t', '').replace('\n', '').replace(' ', '').encode('utf-8')
            flag_one = soup.find('li', attrs={"class": "li-1"})
            if flag_one == None:
                if soup.find('span', class_="mr15") == None:
                    price_zero = soup.find('span', class_="red fs24 mr5").get_text()
                    price = price_zero.replace('\t', '').replace('\n', '').replace(' ', '').encode('utf-8')

                else:
                    tem = soup.find('span', class_="red fs24 mr5").get_text()
                    tem = string.atof(tem)
                    tem = '%.2f' % tem
                    price_zero = tem + soup.find('span', class_="mr15").get_text()
                    price = price_zero.replace('\t', '').replace(' ', '').encode('utf-8')

                    line_zero = soup.find('span', class_="mr5")
                    for i in range(3):
                        line_zero = line_zero.find_next_sibling("span")
                    if line_zero != None:
                        line = line_zero.get_text().encode('utf-8')
                        price = line + ":" + price

            else:
                price_s = []
                price_z = []
                soup.find('ul', attrs={"class": "t clearfix"}).find("li").extract()
                line_zero_s = soup.find('ul', attrs={"class": "t clearfix"}).find_all("li")
                for line_zero in line_zero_s:
                    line = line_zero.get_text().replace('\t', '').replace(' ', '').replace('\n', '').strip()
                    price_zero = soup.find('li', class_="red").get_text().replace('\t', '').replace('\n', '').replace(' ', '')
                    price = line + ":" + price_zero
                    price_s.append(price)
                    soup.find('li', class_="red").extract()
                price = ';'.join(price_s).encode('utf-8')
                for j in price:
                    if j != '\r':
                        price_z.append(j)
                price = ''.join(price_z)



            if mysql_check(id,name,adress) == 0:
                data.append(id)
                data.append(name)
                data.append(time)
                data.append(adress)
                data.append(price)
                global datas
                datas.append(data)
                if len(datas) >= 1000:
                    mysql_insert()
            else:
                data.append(id)
                data.append(name)
                data.append(time)
                data.append(adress)
                data.append(price)
                global updatas
                updatas.append(data)
                if len(updatas) >= 1000:
                    mysql_updata()
    except AttributeError:
        pass


def get_secondLink(info):
    soup = BeautifulSoup(info, "lxml")
    last_page = soup.find(text=re.compile(ur'\u6700\u540E\u9875'))
    if last_page != None:
        next_page = soup.find(text=re.compile(ur'\u4e0b\u4e00\u9875'))
        last_pageUrl = last_page.find_parent()['href']
        next_pageUrl = next_page.find_parent()['href']

        if next_pageUrl != last_pageUrl:
            all_div = soup.find_all('div', attrs={"class": "list-conatiner minH500 mt_20"})
            for div in all_div:
                for a in div.findAll('a', class_="text"):
                    url = a['href']
                    if url not in second_urls:
                        second_urls.append(url)
            next_info = get_content(next_pageUrl)
            get_secondLink(next_info)
        if next_pageUrl == last_pageUrl:
            last_info = get_content(next_pageUrl)
            soup = BeautifulSoup(last_info, "lxml")
            all_div = soup.find_all('div', attrs={"class": "list-conatiner minH500 mt_20"})
            for div in all_div:
                for a in div.findAll('a', class_="text"):
                    url = a['href']
                    if url not in second_urls:
                        second_urls.append(url)
    else:
        pass


def get_link(info):
    soup = BeautifulSoup(info, "lxml")
    all_div = soup.find_all('div', class_="sub-cate")
    for div in all_div:
        for dd in div.find_all('dd'):
            for a in dd.find_all('a'):
                product = a.get_text().encode('utf-8')
                url = a['href']
                global products
                global first_urls
                if url not in first_urls and product != None:
                    products.append(product)
                    first_urls.append(url)



def mysql_HN_init():
    db = MySQLdb.connect("localhost", "root", "wzj20121221", "SPIDER")
    cursor = db.cursor()
    cursor.execute("SELECT VERSION()")
    data = cursor.fetchone()
    print "Mysql has been successfully connected."
    print "Database version : %s" % data
    print "Database:SPIDER"
    print "Create table HN to storage prduct name and it's id."
    sql = """CREATE TABLE HN(
            data_id INT NOT NULL AUTO_INCREMENT,
            product VARCHAR(200) NOT NULL,
            PRIMARY KEY ( data_id )
        )ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
    try:
        cursor.execute(sql)
        print "Table HN is OK."
    except BaseException as e:
        e = str(e)
        e_num = e[1:5]
        if e_num == '1050':
            print "Table HN is OK."
            pass
        else:
            print e
    db.close()


def mysql_HN_check(p):
    flag = 0
    db = MySQLdb.connect("localhost","root","wzj20121221","SPIDER" )
    cursor = db.cursor()
    sql = "SELECT * FROM HN WHERE HN.product='%s'"%p
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        if len(results) != 0:
            flag = 1
    except:
        print "Error: unable to fecth data"
    return flag


def mysql_HN_insert(product):
    db = MySQLdb.connect("localhost", "root", "wzj20121221", "SPIDER")
    cursor = db.cursor()
    sql = "INSERT IGNORE INTO HN(product) \
            VALUES ('%s')" % product
    try:
        cursor.execute(sql)
        db.commit()
    except BaseException as e:

        print e
        db.ollback()
    db.close()


def mysql_selectID(p):
    db = MySQLdb.connect("localhost", "root", "wzj20121221", "SPIDER")
    cursor = db.cursor()
    sql = "SELECT * FROM HN"
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            data_id = row[0]
            product = row[1]
            if product == p:
                return data_id

    except:
        print "Error: unable to fecth data"

    db.close()


def mysql_init(id):
    db = MySQLdb.connect("localhost", "root", "wzj20121221", "SPIDER")
    cursor = db.cursor()
    sql = """CREATE TABLE HN_%s""" %id + """(
                data_id INT NOT NULL AUTO_INCREMENT,
                name VARCHAR(200) NOT NULL,
                updata_time DATETIME NULL,
                adress VARCHAR(100) NOT NULL,
                PRIMARY KEY ( data_id )
            )ENGINE=InnoDB DEFAULT CHARSET=utf8;"""
    try:
        cursor.execute(sql)
        print "Table HN_%s " % id + "is OK."
    except BaseException as e:
        e = str(e)
        e_num = e[1:5]
        if e_num == '1050':
            print "Table HN_%s " % id + "is OK."
            pass
        else:
            print e
    db.close()


def mysql_check(id,name,adress):
    flag = 0
    db = MySQLdb.connect("localhost","root","wzj20121221","SPIDER" )
    cursor = db.cursor()
    sql = "SELECT * FROM HN_%s"%id+" WHERE HN_%s"%id+".name='%s'"%name+"and HN_%s"%id+".adress='%s'"%adress
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        if len(results) != 0:
            flag = 1
    except:
        print "Error: unable to fecth data"
    return flag


def mysql_insert():
    global day
    db = MySQLdb.connect("localhost", "root", "wzj20121221", "SPIDER")
    cursor = db.cursor()
    global datas
    while len(datas) != 0:
        data = datas.pop(0)
        id = data[0]
        name = data[1]
        updata_time = data[2]
        adress = data[3]
        price = data[4]
        sql = "INSERT IGNORE INTO HN_%s" %id+ "(name, updata_time, adress,"+"price_%s"%day+") VALUES ('%s', '%s', '%s','%s')" % (name, updata_time, adress,price)
        try:
            cursor.execute(sql)
            db.commit()
        except BaseException as e:
            print e
            db.ollback()
    db.close()


def mysql_updata():
    global updatas
    while len(updatas) != 0:
        data = updatas.pop(0)
        id = data[0]
        name = data[1]
        updata_time = data[2]
        adress = data[3]
        price = data[4]
    db = MySQLdb.connect("localhost","root","wzj20121221","SPIDER" )
    cursor = db.cursor()
    day = time.strftime("%Y%m%d")
    sql = "update HN_%s"%id+" set updata_time ='%s',"%updata_time+"price_%s"%day+"='%s'"%price+" WHERE name='%s'"%name+" and adress='%s'"%adress
    try:
        cursor.execute(sql)
        db.commit()
    except BaseException as e:
        print e
    db.close()


def mysql_day(id):
    db = MySQLdb.connect("localhost", "root", "wzj20121221", "SPIDER")
    cursor = db.cursor()
    global day
    print "Create Colum price%s "%day+"to storage the %s "%day+"product price."
    sql = "alter table HN_%s"%id + " add price_%s"%day+" varchar(200)"
    try:
        cursor.execute(sql)
        print "Colum price_%s " % day + "is OK."
    except BaseException as e:
        e = str(e)
        e_num = e[1:5]
        if e_num == '1060':
            print "Colum price_%s " % day + "is OK."
            pass
        else:
            print e
    db.close()


def mysql_select(id):
    db = MySQLdb.connect("localhost", "root", "wzj20121221", "SPIDER")
    cursor = db.cursor()
    print "table : HN_%s" % id
    sql = "SELECT * FROM HN_%s" % id
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            data_id = row[0]
            name = row[1]
            updata_time = row[2]
            adress = row[3]
            print "%d\t%s\t%s\t%s" % \
                  (data_id, name, updata_time, adress)

    except:
        print "Error: unable to fecth data"
        db.rollback()
    db.close()



def do_HN_n():
    global first_urls
    global second_urls
    global count
    while len(first_urls) != 0 and count < 4:
        product = products.pop(0)
        id = mysql_selectID(product)
        id = str(id)
        print "Create table to storage data from %s" % product + " which NN.id is %s."%id
        first_url = first_urls.pop(0)
        mysql_init(id)
        mysql_day(id)
        info = get_content(first_url)
        get_secondLink(info)
        count = count + 1

        flag = len(second_urls)
        a = 0
        bar = 0
        print "get data"
        print "0--------10--------20--------30--------40--------50--------60---------70--------90--------90--------100"
        while len(second_urls) != 0:
            second_url = second_urls.pop(0)
            info = get_content(second_url)

            get_data(info,id)
            mysql_insert()
            if a != 100 - len(second_urls) * 100 / flag:
                a = 100 - len(second_urls) * 100 / flag
                while bar < a:
                    bar = bar + 1
                    sys.stdout.write('#')
        mysql_insert()
        mysql_updata()
        mysql_select(id)
        print("\n")


if __name__=='__main__':
    mysql_HN_init()
    info = get_content("http://www.cnhnb.com/")
    get_link(info)
    for product in products:
        if mysql_HN_check(product) == 0:
            mysql_HN_insert(product)
    do_HN_n()