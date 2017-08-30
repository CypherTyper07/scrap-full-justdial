BOT_NAME = 'scraping'

SPIDER_MODULES = ['scraping.spiders']
NEWSPIDER_MODULE = 'scraping.spiders'

MAIL_FROM = "sachan.ranvijay@gmail.com"
MAIL_HOST = "smtp.gmail.com"
MAIL_PORT = 587
MAIL_USER = "sachan.ranvijay@gmail.com"
MAIL_PASS = "test"
MAIL_TO = ["sachan.ranvijay@gmail.com"]
MAIL_CC = ["sachan.ranvijay@gmail.com"]


DOWNLOADER_MIDDLEWARES = {'scraping.middleware.RandomUserAgentMiddleware': 400,
                          'scrapy.contrib.downloadermiddleware.useragent.UserAgentMiddleware': None,
                          }
DOWNLOAD_TIMEOUT = 1200
CONCURRENT_REQUESTS = 1
# Advanced settings for future modifications
# USER_AGENT = 'name'
# REFERER_ENABLED = False
# RETRY_ENABLED = False
