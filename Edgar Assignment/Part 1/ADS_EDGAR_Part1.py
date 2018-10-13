from bs4 import BeautifulSoup
import urllib.request
import csv
import sys
import os
import re
import zipfile
import logging
import time
import datetime
import boto
import boto3
from boto.s3.key import Key
import random

def upload_to_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, inputLocation, filepaths):
    try:
        conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        print("Connected to S3")
    except:
        logging.info("Amazon keys are invalid!!")
        print("Amazon keys are invalid!!")
        exit()

    loc = ''

    if inputLocation == 'APNortheast':
        loc = boto.s3.connection.Location.APNortheast
    elif inputLocation == 'APSoutheast':
        loc = boto.s3.connection.Location.APSoutheast
    elif inputLocation == 'APSoutheast2':
        loc = boto.s3.connection.Location.APSoutheast2
    elif inputLocation == 'CNNorth1':
        loc = boto.s3.connection.Location.CNNorth1
    elif inputLocation == 'EUCentral1':
        loc = boto.s3.connection.Location.EUCentral1
    elif inputLocation == 'EU':
        loc = boto.s3.connection.Location.EU
    elif inputLocation == 'SAEast':
        loc = boto.s3.connection.Location.SAEast
    elif inputLocation == 'USWest':
        loc = boto.s3.connection.Location.USWest
    elif inputLocation == 'USWest2':
        loc = boto.s3.connection.Location.USWest2

    try:
        ts = time.time()
        st = datetime.datetime.fromtimestamp(ts)
        bucket_name = 'edgarpart1' + str(st).replace(" ", "").replace("-", "").replace(":", "").replace(".","")
        bucket = conn.create_bucket(bucket_name, location=loc)
        print("bucket created")
        s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

        print('s3 client created')

        for f in filepaths:
            try:
                s3.upload_file(f, bucket_name, os.path.basename(f))
                print("File successfully uploaded to S3", f, bucket)
            except Exception as detail:
                print(detail)
                print("File not uploaded")
                exit()

    except:
        logging.info("Amazon keys are invalid!!")
        print("Amazon keys are invalid!!")
        exit()


def build_url(cik,dac):
    cik = str(cik)
    dac = str(dac)
    dac_no_dash = re.sub(r'[-]',r'',dac)
    cik = cik.lstrip('0')

    logging.debug('Calling the buildURL function with CIK : {} & DAC : {}'.format(cik,dac))

    url = 'https://www.sec.gov/Archives/edgar/data/{}/{}/{}/-index.htm'.format(cik,dac_no_dash,dac)
    return url

def target_url(url):
    logging.debug('In the targetURL function parsing the URL : {} '.format(url))

    html = urllib.request.urlopen(url)
    soup = BeautifulSoup(html,'html.parser')
    site_tables = soup.find('table',class_='tableFile')
    rows = site_tables.find_all('tr')
    target_url = ''

    for row in rows:
        target_url = row.findNext('a').attrs['href']
        break
    target_url = 'https://www.sec.gov' + target_url

    get_page(target_url)

    print('The target URL is : {}'.format(target_url))

    return target_url

def get_page(url):
    logging.debug('In the getPage function parsing the URL : {}'.format(url))
    try:
        htmlpage = urllib.request.urlopen(url)
        page = BeautifulSoup(htmlpage, "html.parser")
        find_div_tables(page)
    except:
        return None
        logging.debug('In function getPage : Failed to parse the URL : {}'.format(url))

def find_div_tables(page):
    logging.debug('In the findDivTables function parsing the page')
    all_divtables = page.find_all('table')
    find_data_tables(page, all_divtables)
    return 0

def foldername(page):
    title = page.find('filename').contents[0]
    if ".htm" in title:
        foldername = title.split(".htm")
        company = foldername[0]
        return foldername[0]

def zip_dir(path_dir, path_file_zip=''):
    if not path_file_zip:
        path_file_zip = os.path.join(
            os.path.dirname(path_dir), os.path.basename(path_dir) + '.zip')
    with zipfile.ZipFile(path_file_zip, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for root, dirs, files in os.walk(path_dir):
            for file_or_dir in files + dirs:
                zip_file.write(os.path.join(root, file_or_dir),
                               os.path.relpath(os.path.join(root, file_or_dir),
                               os.path.join(path_dir, os.path.pardir)))

def assure_path_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)


def tag_name(param):
    setflag = "false"
    datatabletags = ["background", "bgcolor", "background-color"]
    for x in datatabletags:
        if x in param:
            setflag = "true"
    return setflag

def headertag_name(param):
    setflag="false"
    datatabletags=["center","bold"]
    for x in datatabletags:
        if x in param:
            setflag="true"
    return setflag

def append_table(table):
    table_list = []
    table_rows = table.find_all('tr')
    for tr in table_rows:
        data=[]
        pdata=[]
        printtds=tr.find_all('td')
        for elem in printtds:
            x = elem.text;
            x = re.sub(r"['()]","",str(x))
            #x = re.sub(r"[$]"," ",str(x))
            if(len(x)>1):
                x = re.sub(r"[—]","",str(x))
                pdata.append(x)
        data = ([elem.encode('utf-8') for elem in pdata])
        table_list.append([elem.decode('utf-8').strip() for elem in data])
    return table_list

def find_data_tables(page, all_divtables):
    logging.debug('In the findDataTables function fetching all 10Q Tables')
    count = 0
    allheaders=[]
    for table in all_divtables:
        blue_table_list = []
        trs = table.find_all('tr')
        for tr in trs:
            if tag_name(str(tr.get('style'))) == "true" or tag_name(str(tr)) == "true":
                blue_table_list = append_table(tr.find_parent('table'))
                break
            else:
                tds = tr.find_all('td')
                for td in tds:
                    if tag_name(str(td.get('style'))) == "true" or tag_name(str(td)) == "true":
                        blue_table_list = append_table(td.find_parent('table'))
                        break
            if not len(blue_table_list) == 0:
                break
        if not len(blue_table_list) == 0:
            count += 1
            ptag=table.find_previous('p');
            while ptag is not None and headertag_name(ptag.get('style'))== "false" and len(ptag.text)<=1:
                ptag=ptag.find_previous('p')
                if headertag_name(ptag.get('style'))== "true" and len(ptag.text)>=2:
                    global name
                    name=re.sub(r"[^A-Za-z0-9]+","",ptag.text)
                    if name in allheaders:
                        hrcount += 1
                        hrname=name+"_"+str(hrcount)
                        allheaders.append(hrname)
                    else:
                        hrname=name
                        allheaders.append(hrname)
                        break
            folder_name = foldername(page)

            logging.debug('In the findDataTable function : Folder Created with Folder Name : {}'.format(folder_name))

            path = str(os.getcwd()) + "/" + folder_name
            assure_path_exists(path)

            logging.debug('In the findDataTables function : The path of the CSV files is {}'.format(path))

            if(len(allheaders)==0):
                filename=folder_name+"-"+str(count)
            else:
                filename=allheaders.pop()

            csvname = filename+".csv"
            csvpath = path + "/" + csvname

            with open(csvpath, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.writer(f)
                writer.writerows(blue_table_list)
            zip_dir(path)

    find_data_tables.zipPath = path + '.zip'


def main(systime):
    args = sys.argv[1:]
    cik = ''
    dac = ''
    AWS_ACCESS_KEY = ''
    AWS_SECRET_KEY = ''
    count = 0

    if len(args) == 0:
        cik = '51143'
        dac = '0000051143-13-000007'
        print('No CIK and DAC provided hence default arguments used')

    for arg in args:
        if count == 0:
            cik = str(arg)
        elif count == 1:
            dac = str(arg)
        elif count == 2:
            AWS_ACCESS_KEY = str(arg)
        elif count == 3:
            AWS_SECRET_KEY = str(arg)
        count += 1

    main.log_file_name = 'EDGAR_Part1_Log -- ' + cik + '--' + systime +'.txt'
    logging.basicConfig(filename=main.log_file_name,level=logging.DEBUG, format = '%(asctime)s - %(levelname)s - %(message)s')

    try:
        url = build_url(cik, dac)
        page_code = urllib.request.urlopen(url)
        if (page_code.code == 200):
            target_url(url)
    except:
        logging.debug('The URL entered : {} could not be opened'.format(url))
        print('URL not correct, please check the URL')

    folderName = find_data_tables.zipPath.split('.zip')[0]

    filepaths = []
    filepaths.append(os.path.join(main.log_file_name))
    filepaths.append(os.path.join(folderName + '.zip'))

    #location = ['APNortheast', 'APSoutheast', 'APSoutheast2', 'CNNorth1', 'EUCentral1', 'EU', 'SAEast', 'USWest','USWest2']

    input_location = 'APSoutheast2'

    upload_to_s3(AWS_ACCESS_KEY, AWS_SECRET_KEY,input_location,filepaths)

if __name__ == '__main__':
    ts = time.time()
    systime = datetime.datetime.fromtimestamp(ts).strftime('%Y%m%d_%H%M%S')
    main(systime)