import requests, random, pymongo,logging, time
from fake_useragent import UserAgent
from pyquery import PyQuery as pq


# setting logger module
basic_format = '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(lineno)d - %(message)s'
logging.basicConfig(level= logging.INFO, format=basic_format)
logger = logging.getLogger(__name__)



ua = UserAgent()
headers = {'User-Agent' : ua.random}


# set up database
try:
    client = pymongo.MongoClient("mongodb://phimes:phimes123456@180.76.153.244:27017")
    mydb = client['proxiesdb']
    mycol = mydb['proxies_table']
    client.server_info()
except Exception as e:
    logger.debug('failed to connect mysql db, the error is %s' %(e))

# set up global variables
url = "https://www.xicidaili.com/nn/"

# xici proxies
class Proxy_Spider:

    def __init__(self):
        self.iplist = []

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
                    # temp_ip_tuple = (onedatalist[1] +': ' +onedatalist[2] ,onedatalist[5])
                    one_ip_dict = {'ip':onedatalist[1]+':'+onedatalist[2],'type':onedatalist[5]} #用于mongodb存的字典格式
                    self.iplist.append(one_ip_dict)
                logger.info(url + " has downloaded")
            else:
                logger.info('failed to get the page, the status code is %s' %str(r.status_code))
        except Exception as e:
            logger.warning(e)
            logger.info('failed to get the page, the status code is %s' % str(r.status_code))
        # finally:
            # 添加代理反爬虫
            # if r.status_code == 503:


    def mul_pgs(self, start, end):
        """
        循环获取多页西刺代理的数据
        :param start:起始页
        :param end:结束页
        """
        try:
            for i in range(start, end):
                next_url = url + str(i)
                time.sleep(10)  # 等待10秒，以免被禁
                logger.info('waiting for 10s to get next page.......')
                self.get_onepg(next_url)
                logger.info('%s page has been downloaded' %(str(i)))
            return self.iplist # 注意这里
        except Exception as e:
            logger.warning(e)

    def if_exits(self):
        """
        判断ip是否已存在list中，并写入
        :param iplist:本次爬取得代理iplist
        """
        # 连接数据库
        fetch_res = mycol.find()
        logger.debug(type(fetch_res))
        curr_proxies = [data['ip'] for data in fetch_res]
        logger.debug(curr_proxies[0])
        #对比爬取的数据库里的ip是否重叠
        try:
            duplicate_count = 0
            for ip_dict in self.iplist:
                if ip_dict['ip'] in curr_proxies:
                    logger.debug(type(ip_dict))
                    self.iplist.remove(ip_dict)
                    duplicate_count += 1
                    logger.info(str(ip_dict) + 'has removed from iplist due to it is already stored in database')
                #验证是否可用
            logger.info('%s in total has been removed from iplist' %s(str(duplicate_count)))
                # self.verify_ip(ip_dict, 'https://www.zhihu.com/')
        except Exception as e:
            logger.warning(e)

    def verify_ip(self, ip_dict):
        """
        :param ip:单个获取到得ip tuple
        :param testurl: 用于测试得IP地址
        """
        proxies = {
            ip_dict['type']: "http:\\" + ip_dict['ip'],
        }
        testurl = "https:\\www.baidu.com"
        try:
            r = requests.get(testurl, proxies=proxies, timeout=5)
            if r.status_code != 200:
                self.iplist.remove(ip_dict)
        except Exception as e:
            # logger.info('current ip %s is not useful, drop it!' %str(ip_tuple[0]))
            logger.warning(e)



    def fetch_ip(self, start = 1, end = 31):
        '''
        fetch ip from xici proxies, get first 30 pages by default
        :param start: start page
        :param end: end page
        '''
        try:
            self.mul_pgs(start, end)
            self.if_exits()
            mycol.insert_many(self.iplist)
            logger.info('writing valid ips %s into database' %s(len(self.iplist)))
        except Exception as e:
            logger.warning(e)


    def get_new_proxy(self ):
        fetch = mycol.find()
        fetch_ip = [x['ip'] for x in fetch]
        random.choice(fetch_ip)

    def delete_proxy(self, ip):
        ip_dict = {'ip':ip}
        mycol.delete_one(ip_dict)

if __name__ == '__main__':
    proxySpdier = Proxy_Spider()
    proxySpider.fetch_ip(1,32)
