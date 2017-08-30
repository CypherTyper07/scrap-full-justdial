import traceback
import csv
import datetime
import time
import string
import urllib2
import thread
import unicodedata
from os.path import join
from scrapy import log
from scrapy import signals
from scrapy.spider import BaseSpider
from scrapy.exceptions import CloseSpider
from scrapy.http import Request
from scrapy.xlib.pydispatch import dispatcher
from scraping.utils import sendMail
from scraping import settings


class ProjectBaseSpider(BaseSpider):
    name = "projectbasespider"
    download_delay = 0
    root_path = ""
    input_location = ""
    output_location = ""
    log_location = ""
    error_location = ""
    output_path = ""
    error_path = ""
    start_time = ""
    wait_in_secs = 0
    max_out_file_size = 0
    retry_enabled = 1
    no_of_consecutive_failures = 0
    no_of_allowed_consec_failures = 0
    num_total_requests = 0
    # Stores response stats in following order: successful(200), Failed (all others)
    num_responses = [0, 0]
    num_data_items = 0
    output_flags = 0
    header_array = []
    failed_urls = []
    parsed_data = []
    exit_code = 0
    alert_url = ""
    meta_array = []
    input_file_name = ""
    to_email = ""
    header_index_title = ""
    output_csv_name = ""
    pk_id = ""
    brand_name = ""

    def __init__(self, root_path=None, wait_in_secs=0, max_out_file_size=0, retry_enabled=1, out_columns=None, meta=None,
                 num_consec_failures=0, alert_url=None, name=None, input_file_name=None, to_email=None,
                 header_index_title=None, output_csv_name=None, pk_id=None, input_location=None, output_location=None,
                 log_location=None, error_location=None, brand_name=None, **kwargs):
        if name is not None:
            self.name = name
        super(ProjectBaseSpider, self).__init__(self.name, **kwargs)

        self.download_delay = float(wait_in_secs)

        LOG_FILE = root_path + "/" + log_location + "/" + "%s_%s.log" % (self.name, int(time.time()))
        log.log.defaultObserver = log.log.DefaultObserver()
        log.log.defaultObserver.start()
        log.started = False
        log.start(LOG_FILE, loglevel=log.DEBUG)

        if (meta is not None):
            self.meta_array = [x.strip() for x in meta.split(',') if x.strip() != ""]

        self.header_array.append("Request URL")
        # for x in self.meta_array:
        # self.header_array.append(x)

        for idx, columnId in enumerate(self.MASTER_COLUMN_ID):
            if ((out_columns is None) or (string.find(out_columns, columnId) >= 0) or (
                        (self.multi_match_processing == "row") and (self.multi_match_row_column_index == idx))):
                self.output_flags = 1
                this_header = self.MASTER_COLUMN_NAME[idx]
                if (self.multi_match_processing == "column") and (self.multi_match_column_count[idx] > 1):
                    for i in range(self.multi_match_column_count[idx]):
                        self.header_array.append(this_header + " " + str(i + 1))
                else:
                    self.header_array.append(this_header)
            else:
                self.output_flags = 0

        self.start_time = str(datetime.datetime.now())
        if root_path.endswith("/"):
            root_path = root_path[0:-1]
        self.root_path = root_path
        self.output_location = output_location
        self.error_location = error_location
        self.input_location = root_path + "/" + input_location + "/"
        self.output_path = root_path + "/" + output_location + "/"
        self.error_path = root_path + "/" + error_location + "/"
        self.wait_in_secs = float(wait_in_secs)
        self.max_out_file_size = max_out_file_size
        self.retry_enabled = retry_enabled
        self.no_of_allowed_consec_failures = int(num_consec_failures)
        self.input_file_name = input_file_name
        self.output_csv_name = output_csv_name
        self.brand_name = brand_name

        if pk_id is not None:
            self.pk_id = pk_id
        if header_index_title is not None:
            self.header_index_title = header_index_title
        if alert_url is not None:
            self.alert_url = alert_url
        if to_email is not None:
            self.to_email = [to_email]
        else:
            self.to_email = settings.MAIL_TO

        # self.info("Spider initialized with arguments: root_path:" + root_path + ", 
        # wait_in_secs:" + str(wait_in_secs) + ", max_out_file_size:" + str(max_out_file_size) + ", 
        # retry_enabled:" + str(retry_enabled) + ", out_columns:" + (out_columns if (out_columns is not None) 
        # else "ALL") + ", meta:" + str(self.meta_array) + ", num_consec_failures:" + str(num_consec_failures) + ", 
        # alert_url:" + self.alert_url+", input_file_name: "+input_file_name+", to_email : "+str(to_email))

        dispatcher.connect(self.spider_closed, signal=signals.spider_closed)
        dispatcher.connect(self.spider_error, signal=signals.spider_error)

    def start_requests(self):
        self.info("inside start request")
        input_path = self.input_location
        # inputCSVs = [ f for f in listdir(input_path) if isfile(join(input_path, f)) ]
        inputCSVs = [self.input_file_name]
        self.info("Created By Ranvijay: inputCSVs:" + str(inputCSVs))
        outputCSVs = [self.output_csv_name]
        self.info("Created By Ranvijay: OutputCSVs:" + str(outputCSVs))

        for thisFile in inputCSVs:
            try:
                self.info("Processing Input CSV: " + join(input_path, thisFile))
                varCsv = csv.DictReader(open(join(input_path, thisFile), 'rU'), delimiter=',', quotechar='"')

                thisMeta = {"handle_httpstatus_all": 1}
                if self.retry_enabled <= 0:
                    thisMeta["dont_retry"] = 1

                for line in varCsv:
                    for x in self.meta_array:
                        if x in line:
                            thisMeta[x] = line[x]
                            # line[self.pk_id]

                    yield Request(self.get_formatted_csv_header_val(line[self.header_index_title]), dont_filter=True,
                                  callback=self.parse_response, meta=thisMeta)
                    self.info("after yield requrest")
                    self.num_total_requests = self.num_total_requests + 1
                    # if(self.wait_in_secs > 0):
                    # time.sleep(int(self.wait_in_secs+(round(random.uniform(.1,.9), 1)*self.wait_in_secs)))
            except:
                self.error("Unexpected error:")
                self.error(traceback.print_exc())
                self.error("skipping this file because of the above mentioned error...!!!")

    def download_errback(self, response):
        self.info("ranvijay - Download error callback")
        self.info(type(response), repr(response))
        self.info(repr(response.value))
        self.info("error Call back  :       " + str(type(response), repr(response)))
        self.info("error call back 2    :   " + str(repr(response.value)))

    def spider_closed(self, spider):
        if len(self.parsed_data) != 0:
            self.write_data_csv()
        if len(self.failed_urls) != 0:
            self.write_error_csv()

        body_exit_status = " has completed successfully."
        subject_exit_status = "processing complete"

        if self.exit_code == 1:
            self.info("going to handle failed requuest from parse response !200:alert url:" + self.alert_url)
            body_exit_status = " has been terminated due to high number of failures.\n" \
                               "This process will be disabled and needed to enable it manually.\n"
            subject_exit_status = "processing terminated"
            if self.alert_url != "":
                self.info("going to start new thread")
                thread.start_new_thread(urllib2.urlopen, (urllib2.Request(self.alert_url),))

        mail_body = "Dear Administrator,\n\n\tThe " + \
            self.name + " scheduled at " + \
            self.start_time + body_exit_status + " Following are the detailed statistics:\n\t" \
                                                 "Total requests submitted: " + str(
            self.num_total_requests) + "\n\tSuccessful requests (response code 200) : " + str(
            self.num_responses[0]) + "\n\tFailed requests: " + str(
            self.num_responses[1]) + "\n\tNumber of Output Data Items: " + str(
            self.num_data_items) + "\n\n Thanks,\n Ranvijay"
        sendMail(self.to_email, self.name + ": " + subject_exit_status, mail_body, settings.MAIL_CC)

    def spider_error(self, failure, response, spider):
        self.info("going to handle failed requuest from spider error::")
        self.handle_failed_response(response)

    def handle_failed_response(self, response):
        self.error("Request failed for URL: " + response.url + ", StatusCode: " + str(
            response.status) + " , no_of_allowed_consec_failures: " + str(
            self.no_of_allowed_consec_failures) + " ,no_of_consecutive_failures: " + str(
            self.no_of_consecutive_failures))
        self.failed_urls.append({"url": response.url})
        self.num_responses[1] = self.num_responses[1] + 1

        self.no_of_consecutive_failures = self.no_of_consecutive_failures + 1
        if ((self.no_of_allowed_consec_failures > 0) and (
                    self.no_of_consecutive_failures > self.no_of_allowed_consec_failures)):
            self.exit_code = 1
            raise CloseSpider("Number of consecutive failures exceeded allowed limit")

    def write_data_csv(self):
        output_csv_name = self.output_path + self.output_csv_name
        out_file = open(output_csv_name, "wt")
        writer = csv.DictWriter(out_file, fieldnames=self.header_array)

        headers = {}
        for headerName in self.header_array:
            headers[headerName] = headerName
        writer.writerow(headers)

        writer.writerows(self.parsed_data)
        out_file.close()

    def write_error_csv(self):
        error_csv_name = self.error_path + "%s_err_%s.csv" % (self.name, int(time.time()))
        err_file = open(error_csv_name, "wt")
        writer = csv.DictWriter(err_file, fieldnames=["url"])

        writer.writerow({"url": "url"})
        writer.writerows(self.failed_urls)
        err_file.close()

    def info(self, text):
        log.msg(text, level=log.INFO, spider=self)

    def error(self, text):
        log.msg(text, level=log.ERROR, spider=self)

    def debug(self, text):
        log.msg(text, level=log.DEBUG, spider=self)

    @staticmethod
    def to_unicode(text):
        try:
            return text.decode("utf-8").encode("ascii", "xmlcharrefreplace")
        except UnicodeDecodeError:
            return text.decode("ascii", "ignore")

    @staticmethod
    def remove_accent(data):
        return unicodedata.normalize('NFKD', data).encode('ASCII', 'ignore')
