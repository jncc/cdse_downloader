import luigi
import requests
import logging
import json
import os
from functional import seq
from pprint import pformat

log = logging.getLogger("luigi-interface")

class SearchForProductsFromList(luigi.Task):
    stateLocation = luigi.Parameter()
    productListFile = luigi.Parameter()
    
    searchUrl = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products/OData.CSC.FilterList"
    
    def formatProductName(self, product):
        if product.rfind(".SAFE") == -1:
            product = f"{product}.SAFE"
        
        return product
        
    def run(self):
        
        productList = seq(open(self.productListFile)) \
                        .map(lambda line: str(line).rstrip('\n')) \
                        .filter_not(lambda line: str.strip(line) == "") \
                        .map(lambda product: {"Name" : self.formatProductName(product)}) \
                        .to_list()
        
        response = requests.post(
            url=self.searchUrl,
            json={"FilterProducts" : productList}
            )
        
        if response.status_code != 200:
            log.error("cdse API call failed")
            log.error(f"api responded with: {pformat(response.json())}")
            
            raise Exception("CDSE API call failed see log for details")
        
        results = response.json()
        
        features = seq(results["value"]) \
                    .map(lambda r: {"productID": r["Name"].replace(".SAFE", ""),
                                    "productPath": r["S3Path"],
                                    "onlineStatus": 'ONLINE' if r["Online"] else 'OFFLINE',
                                    "productCheckSum": ""}) \
                    .to_list()
        
        if len(productList) != len(features):
            msg = "The number of products returned does not match the number requested"
            
            missingProducts = seq(productList) \
                                .map(lambda x: x["Name"].replace(".SAFE", "")) \
                                .difference(seq(features)
                                            .map(lambda f: f"{f['productID']}")) \
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