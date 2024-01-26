import requests
# import pandas as pd
from pathlib import Path
import sys
import argparse
import math
import logging
import json
from pprint import pformat
from functional import seq

log = logging.getLogger("luigi-interface")

class QueryCDSE:
    
    def __init__(self, sat):
        if sat not in [1,2]:
            raise Exception("sat must be 1 or 2")
        
        self.url = f'https://catalogue.dataspace.copernicus.eu/resto/api/collections/Sentinel{sat}/search.json'


    @staticmethod
    def parse_features(features, rel_orbit_deny_list):
        
        return seq(features) \
            .where(lambda f: rel_orbit_deny_list is None \
                or f["properties"]["relativeOrbitNumber"] not in rel_orbit_deny_list) \
            .select(lambda f: {
                "productID": f["properties"]["title"].replace(".SAFE", ""),
                "productPath": f["properties"]["productIdentifier"],
                "onlineStatus": f["properties"]["status"],
                "productCheckSum": "", # May be used in the future
            }) \
            .to_list()
        
    def paged_query(self, count):
        """
        submit query with 'page' query parameter to handle cases where the number of returned records exceeds the max of records returned
        Note, the api is limited to 2000 max records. Some queries will exceed this.
        """

        start_record_index = ((count -1) * self.maxrecords) + 1
        end_record_index = count  * self.maxrecords

        log.info(f"Querying CDSE Page {count} of {self.query_count} :: Records Index = {start_record_index} to {end_record_index}")

        self.params.update({
            'page': count
        })

        response = requests.get(
            self.url,
            params=self.params,
            )
        
        if response.status_code != 200:
            log.error(f"cdse API call failed on page {count}")
            log.error(f"api responded with: {pformat(response.json())}")
            
            raise Exception("CDSE API call failed see log for details")
        
        results = response.json()

        return results['features']
    
    def run_query(self, params, rel_orbit_deny_list):
        """
        Submit an intial query, parse the total records and iterate through the paged queries (&page=nnn) to make
        concatentate list of dataframes per paged query
        if required, export to csv and remove records matching the relative orbit deny list, i.e. to filter those out.
        """
        self.params = params
        
        self.maxrecords = params["maxRecords"]

        # ensure we get a count of available records
        self.params.update({
            'exactCount' : True,
        })
        
        log.debug(f"params {pformat(self.params)}")

        # do an initial query to return the record count
        response = requests.get(
            self.url,
            params=self.params,
            timeout=30,
        )
        
        if response.status_code != 200:
            log.error("cdse API call failed")
            log.error(f"api responded with: {pformat(response.json())}")
            
            raise Exception("CDSE API call failed see log for details")
        
        results = response.json()
                                    
        num_records = results['properties']['totalResults']
        log.info(f'Querying CDSE :: Matched records = {num_records}')

        list_features = []
        # if record count exceeds max records per query, then iterate through pages of api responses
        if num_records > self.maxrecords:

            self.query_count = math.ceil(int(num_records) / int(self.maxrecords))

            for count in range(1, self.query_count + 1):
                features = self.paged_query(count)
                list_features.extend(self.parse_features(features, rel_orbit_deny_list))
            
        elif num_records > 0:
            features = results['features']
            list_features.extend(self.parse_features(features, rel_orbit_deny_list))
            
        return list_features
