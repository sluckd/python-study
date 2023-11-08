import pymysql
from urllib.request import urlopen
from bs4 import BeautifulSoup as BS
from datetime import datetime, timedelta

# 假期对象
holObj = {}
# 工作日对象
workDayObj = {}
# url 将来只替换这部分就行
url = 'https://www.gov.cn/zhengce/content/202310/content_6911527.htm'
#url='https://www.gov.cn/zhengce/content/2022-12/08/content_5730844.htm'

sql = []
now = datetime.now()
year = 0


def getDate(str):
    month = str[:str.index('月')]
    day = str[str.index('月')+1:str.index('日')]
    month = "{:0>2}".format(month)
    day = "{:0>2}".format(day)
    date = "{}-{}-{}".format(year, month, day)
    return date

def getDateDelta(date_str1,date_str2,dts):
    # 将日期字符串转换为datetime对象
    date_format = "%Y年%m月%d日"
    date1 = datetime.strptime(date_str1, date_format)
    date2 = datetime.strptime(date_str2, date_format)

    # 计算日期间隔
    date_delta = date2 - date1

    # 遍历并打印日期间隔中的日期
    for i in range(date_delta.days + 1):
        current_date = date1 + timedelta(days=i)
        dts.append(current_date.strftime('%Y-%m-%d'))

def getDateList(param):
    dts = []
    cnyear = str(year) + '年'
    print(param)
    if param.count('至') == 0:
        dts.append(getDate(param)) 
    else:
        arr = param.split('至')
        arr1 = arr[0]
        arr2 = arr[1]
        arr2 = arr2[0:arr2.index('日')+1]

        if arr1.count('年') == 0:
            arr1 = cnyear + arr1
        if arr2.count('月') == 0:
            arr2 = cnyear + str(datetime.strptime(arr1, '%Y年%m月%d日').month)+'月' + arr2
        elif arr2.count('年') == 0:
            arr2 = cnyear + arr2    

        getDateDelta(arr1,arr2,dts) 
    return dts


def deal_sql(holname, line):
    # 冒号+逗号截取放假日期
    fjword = line[line.index('：')+1: line.index('，')]
    jnum = line.index('。')+1
    # 处理工作日
    workday = ''
    if line.count('。') > 1:
        workday = line[jnum: line.index('。', jnum+2)]
        workdayArr = workday.split('、')
        dts = []
        for str in workdayArr:
            dts.append(getDate(str))
        workDayObj[holname] = dts

    if (fjword.count('至') == 0):  # 单独一天
        holObj[holname] = getDateList(fjword)
    else:
        holObj[holname] = getDateList(fjword)

# 读文件的方式
def readFile():
    file = open('D:\work\pythonspace\holday.txt', 'r+', -1, 'utf-8')
    tul = ('一', '二', '三', '四', '五', '六', '七', '八', '九')

    for line in file:
        if (len(line) == 1 or line == ''):
            continue
        if (line[0:1] not in tul):
            continue

        holname = line[line.index("、") + 1:line.index("：")]
        deal_sql(holname, line)

    file.close()

#readFile()
def readUrl():
    myUrl = urlopen(url)
    content = myUrl.read()
    bs = BS(content)
    ucap_content = bs.find(attrs={"id":"UCAP-CONTENT"}).find_all("p")
    tul = ('一', '二', '三', '四', '五', '六', '七')
 
    for p in ucap_content:
        line = p.text
        if line.count('国务院办公厅关于'):
            endindex = line.index('年')
            global year 
            year = line[endindex-4:endindex]
            print (year)
        if (len(line) == 1 or line == ''):
            continue
        if (line[0:1] not in tul):
            continue

        holname = line[line.index("、") + 1:line.index("：")]
        deal_sql(holname, line)
 
readUrl()

sqlformat = "INSERT INTO `dict_hol_date` (`cust_id`, `h_day`, `dt`, `date_type`) VALUES (0, '{}', '{}', {});"


def createSql(obj, list, type):
    for holname in obj.keys():
        dts = obj[holname]
        for date in dts:
            sql = sqlformat.format(holname, date, type)
            list.append(sql)


sqls = []
createSql(holObj, sqls, 3)
createSql(workDayObj, sqls, 1)

# 插入数据库

# 打开数据库连接
db = pymysql.connect(host='ip',
                     port=3306,
                     user='',
                     password='',
                     database=''
                     )

cursor = db.cursor()
try:
    # 检查对应年份数据是否存在，如果存在则不执行
    cursor.execute(
        'select count(1) from dict_hol_date where dt like \'{0}%\''.format(year))
    count = cursor.fetchone()
    print(count)
    if count[0] > 0:
        print("数据已经存在")
        exit()

    for sql in sqls:
         cursor.execute(sql)
         print(sql)
    print("sql 执行完毕")
except Exception as e:
    db.rollback()
    print("数据回滚", e)
else:
    db.commit()
    print("提交数据")
finally:
    db.close()
    print("执行完毕，关闭数据库连接。")


# print(holObj)
# print(workDayObj)
