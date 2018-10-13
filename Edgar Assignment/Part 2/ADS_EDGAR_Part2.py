import urllib.request as u
import pandas as pd
import os
import boto
import boto3
import time
import datetime
import sys
import logging
import zipfile
from io import BytesIO
from zipfile import ZipFile
from bs4 import BeautifulSoup
import glob

def upload_to_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, inputLocation, filepaths):
    try:
        conn = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        print("S3 Connection Established")
    except:
        logging.info("AWS S3 Keys Invalid")
        print("AWS S3 Keys Invalid")
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
        bucket_name = 'edgarpart2' + str(st).replace(" ", "").replace("-", "").replace(":", "").replace(".","")
        bucket = conn.create_bucket(bucket_name, location=loc)
        print("bucket created")
        s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY)

        print('S3 Boto Instance Created')

        for f in filepaths:
            try:
                s3.upload_file(f, bucket_name, os.path.basename(f))
                print("File successfully uploaded to S3", f, bucket)
            except Exception as detail:
                print(detail)
                print("File not uploaded")
                exit()

    except:
        logging.info("AWS S3 Keys Invalid")
        print("AWS S3 Keys Invalid")
        exit()

def websrape(page, year):
    soup = BeautifulSoup(page,'html.parser')
    name_box = soup.findAll('div', attrs={'id':'asyncAccordion'})

    for aTag in name_box:
        aTagList = aTag.findAll("a")
        asd=[]
        for aTag in aTagList:
            hrefTagList = aTag.get('href')
            asd.append("https://www.sec.gov" + hrefTagList)
        zipFinalListAll = []

        for zipList in asd:
            if str(year) in zipList:
                linkhtml = u.urlopen(zipList)
                allzipfiles = BeautifulSoup(linkhtml, "html.parser")
                zipListAll = allzipfiles.find_all('a')
                zipFinalListAll.append(zipListAll)
            # else:
            #     print("No data available for " + year + " on edgar")

        z=zipFinalListAll[0]
        all_days_links = []

        for aTag in z:
            hrefTagList = aTag.get('href')
            all_days_links.append(hrefTagList)
        first_day_of_month = []

        for i in all_days_links:
            if '01.' in i:
                first_day_of_month.append(i)

    downloadZipFilesToSystem(first_day_of_month)

def downloadZipFilesToSystem(first_day_of_month):
    path = str(os.getcwd()) + "\\Downloaded"
    for first in first_day_of_month:
        with u.urlopen(first) as zipFirstMonth:
            with ZipFile(BytesIO(zipFirstMonth.read())) as zipFirstMonthFile:
                 zipFirstMonthFile.extractall(path)
    getCSVFiles(path)

def getCSVFiles(path):
    allFiles = glob.glob(path + "/*.csv")
    folder_path = allFiles[0][-12:].split('.')[0][:-4]
    # list_ = []
    for file_ in allFiles:
        df0 = pd.read_csv(file_,index_col=None, header=0, low_memory= False)
        # list_.append(df)
        if df0.empty == False:
            df1 = change_dataTypes(df0)
            df2 = missingValueAnalysis(df1)
            evaluateFile(str(file_), df2)
        else:
            logging.debug('No data for '+file_)

    zip_dir(path+'/'+folder_path)

def change_dataTypes(ndf):
    new_data = pd.DataFrame()
    logging.debug('In the function : change_dataTypes')
    ndf['zone'] = ndf['zone'].astype('int64')
    ndf['cik'] = ndf['cik'].astype('int64')
    ndf['code'] = ndf['code'].astype('int64')
    ndf['idx'] = ndf['idx'].astype('int64')
    ndf['noagent'] = ndf['noagent'].astype('int64')
    ndf['norefer'] = ndf['norefer'].astype('int64')
    ndf['crawler'] = ndf['crawler'].astype('int64')
    ndf['find'] = ndf['find'].astype('int64')
    new_data = ndf
    return new_data

def missingValueAnalysis(ndf):

    logging.debug('Performing Missing Value Analysis')
    newDataframe = pd.DataFrame()

    ndf.loc[ndf['extention'].isin(['.txt', '.htm', '.paper', '.hdr.sgml', '.xml']), 'extention'] = ndf["accession"].map(str) + ndf["extention"]
    ndf['browser'] = ndf['browser'].fillna(value = 'Not Known', axis=0)
    ndf['size'] = ndf['size'].fillna(0)
    ndf['size'] = ndf['size'].astype('int64')
    ndf = pd.DataFrame(ndf.join(ndf.groupby('cik')['size'].mean(), on='cik', rsuffix='_new'))
    ndf['size_new'] = ndf['size_new'].fillna(0)
    ndf['size_new'] = ndf['size_new'].astype('int64')
    ndf.loc[ndf['size'] == 0, 'size'] = ndf.size_new
    del ndf['size_new']
    newDataframe = ndf
    return newDataframe

def confirm_path(path):
    if not os.path.exists(path):
        os.makedirs(path)

def zip_dir(folder_dir, path_file_zip=''):
    if not path_file_zip:
        path_file_zip = os.path.join(
            os.path.dirname(folder_dir), os.path.basename(folder_dir) + '.zip')
    with zipfile.ZipFile(path_file_zip, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for root, dirs, files in os.walk(folder_dir):
            for file_or_dir in files + dirs:
                zip_file.write(os.path.join(root, file_or_dir),
                               os.path.relpath(os.path.join(root, file_or_dir),
                                               os.path.join(folder_dir, os.path.pardir)))

def evaluateFile(file_,ndf):
    year_folder_name = file_[-12:].split('.')[0][:-4]
    month_folder_name = file_[-12:].split('.')[0][4:][:2]
    path = str(os.getcwd()) +'/'+ year_folder_name + '/' + month_folder_name

    confirm_path(path)

    # Sorting top 10 Ciks with 404 errors
    top404Cik = ndf[ndf['code'] == 404].groupby('cik')['cik'].count().sort_values(ascending = False)
    top404Cik.to_csv(path+'/'+"Top404.csv", header= True)

    # Sorting top 10 FileSize
    top10DocSize = ndf[['extention', 'size']].sort_values(by='size', ascending=False).reset_index().head(10)
    top10DocSize.to_csv(path+'/'+"TopDocSize.csv", header=True)

    # Summary statistic for Company by a particular IP
    ipSummary = ndf['ip'].groupby(ndf['cik']).describe()
    ipSummary.to_csv(path+'/'+"IPStatistic.csv", header=True)

    # Summary statistic for a second by IPs
    timeSummary = ndf['ip'].groupby(ndf['time']).describe()
    timeSummary.to_csv(path+'/'+ "SecondsStatistic.csv", header=True)

def valid_year(year):
    if year.isdigit():
        if int(year) >= 2003 and int(year) <= 2017:
            return year
        else:
            print("Year is invalid")
            exit()
    else:
        print("Year is not integer")
        exit()

def main(systime):
    args = sys.argv[1:]
    year = ''
    AWS_ACCESS_KEY = ''
    AWS_SECRET_KEY = ''
    count = 0

    if len(args) == 0:
        year = '2003'
        #print('No year provided hence latest year 2017 is used')

    for arg in args:
        if count == 0:
            year = str(arg)
        elif count == 1:
            AWS_ACCESS_KEY = str(arg)
        elif count == 2:
            AWS_SECRET_KEY = str(arg)
        count += 1

    year = valid_year(year)

    log_file_name = 'EDGAR_Part2_Log -- ' + year + '--' + systime + '.txt'
    logging.basicConfig(filename=log_file_name, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

    try:
        page = u.urlopen('https://www.sec.gov/dera/data/edgar-log-file-data-set.html')
        if (page.code == 200):
            websrape(page, year)
            #getCSVFiles(str(os.getcwd()) + '//Downloaded')
    except:
        logging.debug('The year entered : {} could not be opened'.format(page))
        print('URL not correct, please check the URL')

    filepaths = []
    filepaths.append(os.path.join(log_file_name))
    filepaths.append(os.path.join('Downloaded'+'/'+year + '.zip'))

    input_location = 'APSoutheast2'

    upload_to_s3(AWS_ACCESS_KEY, AWS_SECRET_KEY, input_location, filepaths)

if __name__ == '__main__':
    ts = time.time()
    systime = datetime.datetime.fromtimestamp(ts).strftime('%Y%m%d_%H%M%S')
    main(systime)