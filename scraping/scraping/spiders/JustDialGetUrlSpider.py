from scraping.spiders.project_base_spider import ProjectBaseSpider

import urllib


class JustDialGetUrlSpider(ProjectBaseSpider):
    name = "JustDialGetUrlSpider"

    # How spider should process the multiple matching columns. Allowed values are "none", "row" and "column"
    multi_match_processing = "row"
    # For each column name, number of multiple matches to add as columns, if "column" value is set above
    multi_match_row_column_index = 0

    MASTER_COLUMN_ID = ["url"]
    MASTER_COLUMN_NAME = ["url"]

    def parse_response(self, response):
        self.info("in parse response")
        if response.status != 200:
            self.info("going to handle failed requuest from parse response !200::")
            self.handle_failed_response(response)
            data_row = {}
        if (self.output_flags == 1):
            for url_path in response.xpath\
                        ("//*[@class='rslwrp']/section/section[2]/section/aside/p/span[@class='jcn dcomclass' or @class='jcn ']"):
                data_row = {}
                href_val = url_path.xpath("a/@href").extract()[0]
                self.info("urlsssssssssssssssss   : " + str(href_val))
                data_row['url'] = href_val
                data_row["Request URL"] = self.to_unicode(response.url.strip(' \t\n\r'))
                self.parsed_data.append(data_row)

        if ((self.max_out_file_size > 0) and ((int(self.num_responses[0]) % int(self.max_out_file_size)) == 0)):
            self.write_data_csv()
            self.parsed_data = []

    def __init__(self, root_path=None, wait_in_secs=10, max_out_file_size=0, retry_enabled=1, out_columns=None,
                 meta=None, num_consec_failures=5, alert_url="http://www.justdial.com/",
                 input_file_name='JustDialSearchInput.csv', to_email=None, header_index_title='url',
                 ouput_csv_name='JustDialUrls.csv', pk_id=None, input_location=None,
                 output_location=None, log_location=None, error_location=None, **kwargs):
        super(JustDialGetUrlSpider, self).__init__(root_path, wait_in_secs, max_out_file_size, retry_enabled,
                                                   out_columns, meta, num_consec_failures, alert_url, self.name,
                                                   input_file_name, to_email, header_index_title, ouput_csv_name,
                                                   pk_id, input_location, output_location, log_location, error_location,
                                                   **kwargs)

    def get_formatted_csv_header_val(self, url_from_csv):
        newurl_from_csv = ""
        new_unquote_url = urllib.unquote(url_from_csv)
        if (not new_unquote_url.startswith("http://") and not new_unquote_url.startswith("https://")):
            newurl_from_csv = "http://" + new_unquote_url
        else:
            newurl_from_csv = new_unquote_url
        return newurl_from_csv
