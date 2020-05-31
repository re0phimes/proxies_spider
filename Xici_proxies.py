import requests, random, pymongo,logging, pymysql, time
from fake_useragent import UserAgent
from pyquery import PyQuery as pq


ua = UserAgent()
headers = {'User-Agent' : ua.random}
logger = logging.getLogger(__name__)

# set up database
mysqldb = pymysql.connect('180.76.153.244','root','123456','mysql_proxies')
cursor = mysqldb.cursor()
cursor.execute('select VERSION()')

# set up global variables
url = "https://www.xicidaili.com/nn/"
# iplist = []

# xici proxies
class Find_Proxies:

    def __init__(self):
        self.iplist = []
    #     self.proxies = {}



    def get_onepg(self ,url):
        """
        获取一页的代理数据
        :param url:西刺代理主页
        """
        #         proxies = self.get_proxies() #从数据库中获取一个代理
        try:
            r = requests.get(url, headers=headers ,timeout=10)
            # print(r.status_code)
            if r.status_code == 200:
                doc = pq(r.text)
                ips = doc('tr')
                for one_set_data in list(ips.items())[1:]:
                    onedatalist = one_set_data('td').text().split(' ')
                    temp_ip_tuple = (onedatalist[1 ] +': ' +onedatalist[2] ,onedatalist[5])
                    #                     temp_ip_dict = {'ip':onedatalist[1]+':'+onedatalist[2],'type':onedatalist[5]} #用于mongodb存的字典格式
                    self.iplist.append(temp_ip_tuple)
            else:
                print('failed to get the page, the status code is %s' %str(r.status_code))
        except Exception as e:
            logger.warning(e)
            logger.warning('failed to get the page, the status code is %s' % str(r.status_code))

    def mul_pgs(self, start, end):
        """
        循环获取多页西刺代理的数据
        :param start:起始页
        :param end:结束页
        """
        try:
            for i in range(start, end):
                next_url = url + str(i)
                print(next_url)
                time.sleep(10)  # 等待10秒，以免被禁
                self.get_onepg(next_url)
                print('%s page has been downloaded' %(str(i)))
            return self.iplist # 注意这里
        except Exception as e:
            logger.warning(e)
            logger.warning('mul_pgs failed')

    def if_exits(self):
        """
        判断ip是否已存在list中
        :param iplist:本次爬取得代理iplist
        """
        # 连接数据库
        try:
            #             curr_proxies = list(db.find({},{'_id':0,'ip':1}))
            sql = 'select ip from xici'
            if cursor.execute(sql):
                fetch_res = cursor.fetchall()
                curr_proxies = [data[0] for data in fetch_res]
            else:
                print("failed to get data")
            for ip in self.iplist:
                if ip[0] in curr_proxies:
                    self.iplist.remove(ip)
                    self.verify_ip(ip, 'https://www.zhihu.com/')
            # 插入
            insert_sql = 'INSERT INTO xici(ip,type) VALUES(%s,%s)'
            tuple_data = tuple(self.iplist)
            cursor.executemany(insert_sql, tuple_data)
            mysqldb.commit()
            print('inserted %d documents of valid ip' %(len(self.iplist)))
            mysqldb.close()
        except Exception as e:
            logger.warning(e)
            logger.warning('check ip exits error')

    def verify_ip(self, ip, testurl):
        """
        :param ip:单个获取到得ip tuple
        :param testurl: 用于测试得IP地址
        """
        if ip['type'] == 'http':
            try:
                r = requests.get(testurl, proxies={'http': ip['ip']}, timeout=5)
            except:
                time.sleep(5)
                if r.status_code != 200:
                    iplist.remove(ip)
        else:
            try:
                r = requests.get(testurl, proxies={'https': ip['ip']}, timeout=5)
            except:
                time.sleep(5)
                if r.status_code != 200:
                    iplist.remove(ip)


    def main_func(self, start, end):
        self.mul_pgs(start, end)
        currIplist = self.if_exits()
        # 写入
        #             client = pymongo.MongoClient("mongodb://phi:Project0925@localhost:27017")
        insert_sql = 'INSERT INTO xici(ip,type) VALUES(%s,%s)'
        tuple_data = tuple(currIplist)
        cursor.executemany(insert_sql, tuple_data)
        mysqldb.commit()
        print('inserted %d documents of valid ip' %(len(currIplist)))
        mysqldb.close()

    def get_proxies(self):
        '''
        从服务器的数据库获取一个代理IP
        '''
        #         client = pymongo.MongoClient("mongodb://testuser:123456@localhost:27017")
        sql = 'select ip from xici'
        if cursor.execute(sql):
            fetch_res = cursor.fetchall()
            curr_proxies = [data[0] for data in fetch_res]  # 合并提取出来的ip到iplist中
            self.iplist = list(set(self.iplist + curr_proxies))
        ip = random.choice(self.iplist)
        return ip

        # 检测代理池中IP数量，少于X就要重新获取。

    def count_valid_proxies(self):
        #         client = pymongo.MongoClient("mongodb://testuser:123456@localhost:27017")
        iplist = db.find({}, {'_id': 0, 'ip': 1})
        if len(iplist) < 100:
            self.main_func()



if __name__ == '__main__':
    Proxy_Spider = Find_Proxies()
    # tempIp = Proxy_Spider.get_proxies()
    # print(tempIp, Proxy_Spider.iplist)
    Proxy_Spider.mul_pgs(11,15)
    Proxy_Spider.if_exits()
    print(Proxy_Spider.iplist)