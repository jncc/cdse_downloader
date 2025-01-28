import luigi
import requests
import logging
import json
import os
from functional import seq
from pprint import pformat
from pystac_client import Client

log = logging.getLogger("luigi-interface")

class SearchForProductsFromList(luigi.Task):
    stateLocation = luigi.Parameter()
    productListFile = luigi.Parameter()
    
    stacUrl = "https://catalogue.dataspace.copernicus.eu/stac/"
    stacCatalog = Client.open(stacUrl)
        
    def run(self):
        
        productList = seq(open(self.productListFile)) \
                        .map(lambda line: str(line).rstrip('\n')) \
                        .map(lambda line: f"{line}.SAFE" if not line.endswith(".SAFE") else line) \
                        .filter_not(lambda line: str.strip(line) == "") \
                        .set()
        
        search = self.stacCatalog.search(ids=productList)
        results = list(search.items())
        
        features = seq(results) \
                    .map(lambda r: {"productID": r.id.replace(".SAFE", ""),
                                    "productPath": r.assets["PRODUCT"].extra_fields.get("alternate")["s3"]["href"],
                                    "onlineStatus": r.assets["PRODUCT"].extra_fields.get("alternate")["s3"]["storage:tier"]}) \
                    .to_list()
        
        if len(productList) != len(features):
            msg = "The number of products returned does not match the number requested"
            
            missingProducts = seq(productList) \
                                .map(lambda x: x.replace(".SAFE", "")) \
                                .difference(seq(features)
                                            .map(lambda f: f['productID'])) \
                                .to_list()
                     
            log.error(msg)
            log.error(f"Missing product IDs: \n {pformat(missingProducts)}")
            raise Exception(f"{msg} - see log for details")
                    
        output = {
            "productList": features
        }

        with self.output().open("w") as outFile:
            outFile.write(json.dumps(output, indent=4, sort_keys=True))
    
    def output(self):
        return luigi.LocalTarget(os.path.join(self.stateLocation, "SearchForProductsByList.json"))