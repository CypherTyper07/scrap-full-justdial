from spiders.JustDialBaseSpider import ProjectBaseSpider

import urllib


class JustDialDetailPageSpider(ProjectBaseSpider):
    name = "JustDialDetailPageSpider"

    # How spider should process the multiple matching columns. Allowed values are "none", "row" and "column"
    multi_match_processing = "row"
    # For each column name, number of multiple matches to add as columns, if "column" value is set above
    multi_match_row_column_index = 0

    MASTER_COLUMN_ID = ["title", "phoneNo", "address", "rating", "comment"]
    MASTER_COLUMN_NAME = ["title", "phoneNo", "address", "rating", "comment"]

    def parse_response(self, response):
        self.info("in parse response")
        if response.status != 200 and response.status != 404:
            self.info("going to handle failed requuest from parse response !200::")
            self.handle_failed_response(response)
            data_row = {}
            data_row["Request URL"] = self.to_unicode(response.url.strip(' \t\n\r'))
            self.sanitizeData(data_row)
            self.parsed_data.append(data_row)
            if ((self.max_output_file_size > 0) and (
                        (int(self.num_responses[0]) % int(self.max_output_file_size)) == 0)):
                self.write_data_csv()
                self.parsed_data = []
            return

            # Rest of the function is executed only for successful responses (200)
        self.noOfConsecutiveFailures = 0
        self.num_responses[0] = self.num_responses[0] + 1

        # Ignore ERROR pages
        if (self.is_valid_response(response) == 0):
            return
        if (self.output_flags == 1):
            data_row = {}
            data_row["Request URL"] = self.to_unicode(response.url.strip(' \t\n\r'))

            for phone in response.xpath(
                    "//aside[@class='compdt']/p[@class='jrcw']/span/a[@class='tel']/b/text()").extract():
                self.info("phoneNo : " + str(phone))
                data_row['phoneNo'] = phone[0].strip(' \t\n\r')

            for address in response.xpath(
                    "//aside[@class='compdt']/p[@class='jaddw ']/span[@class='jaddt']/span/text()").extract():
                self.info("address : " + str(address))
                data_row['address'] = address[0].strip(' \t\n\r')

            for rating in response.xpath(
                    "//div[@class='trstfctr']/ul/li[@class='fctrtng']/a/span[@class='fctrnam']/b/span/span/text()").extract():
                self.info("rating : " + str(rating))
                data_row['rating'] = rating[0].strip(' \t\n\r')

            for title in response.xpath(
                    "//aside[@class='jcnlt']/h1/span[@class='item']/span[@class='fn']/text()").extract():
                self.info("titleee : " + str(title))
                data_row['title'] = title[0].strip(' \t\n\r')

                self.parsed_data.append(data_row)

        if ((self.max_output_file_size > 0) and ((int(self.num_responses[0]) % int(self.max_output_file_size)) == 0)):
            self.write_data_csv()
            self.parsed_data = []

    def __init__(self, root_path=None, wait_in_secs=5, max_output_file_size=0, retry_enabled=1, out_columns=None,
                 meta=None,
                 num_consec_failures=3, alert_url='http://www.justdial.com/', input_file_name='JustDialUrls.csv',
                 to_email=None, header_index_title='url', ouput_csv_name='JustDialDetailInfo.csv', pk_id=None,
                 input_location=None, output_location=None, log_location=None, error_location=None, **kwargs):
        super(JustDialDetailPageSpider, self).__init__(root_path, wait_in_secs, max_output_file_size, retry_enabled,
                                                       out_columns, meta, num_consec_failures, alert_url, self.name,
                                                       input_file_name, to_email, header_index_title, ouput_csv_name,
                                                       pk_id, input_location, output_location, log_location,
                                                       error_location, **kwargs)

    def get_formatted_csv_header_val(self, url_from_csv):
        new_url_from_csv = ""
        new_unquote_url = urllib.unquote(url_from_csv)
        if not new_unquote_url.startswith("http://") and not new_unquote_url.startswith("https://"):
            new_url_from_csv = "http://" + new_unquote_url
        else:
            new_url_from_csv = new_unquote_url
        return new_url_from_csv
