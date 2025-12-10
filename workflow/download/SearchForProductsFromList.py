import luigi
import json
import os
import logging

from pystac_client import Client
from functional import seq
from pprint import pformat
from luigi import LocalTarget
from dotenv import load_dotenv


log = logging.getLogger('luigi-interface')

class SearchForProductsFromList(luigi.Task):
    stateLocation = luigi.Parameter()
    productListFile = luigi.Parameter()
    envFilePath = luigi.Parameter(default=None)

    def run(self):
        if self.envFilePath:
            load_dotenv(self.envFilePath)
        else:
            load_dotenv()

        productList = seq(open(self.productListFile)) \
                        .map(lambda line: str(line).rstrip('\n')) \
                        .filter_not(lambda line: str.strip(line) == "") \
                        .set()
        
        stacUrl = os.getenv("STAC_API_URL")
        bucketName = os.getenv("AWS_BUCKET_NAME")

        log.info(f"Searching with STAC endpoint {stacUrl}")
        stacCatalog = Client.open(stacUrl)
        search = stacCatalog.search(ids=productList)
        results = list(search.items())
        
        products = seq(results) \
                    .map(lambda r: {"productID": r.id,
                                    "remotePath": r.assets["product_metadata"].href
                                        .replace(f"s3://{bucketName}/", "")
                                        .replace("/MTD_MSIL1C.xml", "")}) \
                    .to_list()
        
        if len(productList) != len(products):
            missingProducts = seq(productList) \
                                .difference(seq(products)
                                            .map(lambda f: f['productID'])) \
                                .to_list()
            
            log.error(f"Missing product IDs: \n {pformat(missingProducts)}")
            raise Exception("The number of products returned does not match the number requested")
                    
        output = {
            "productList": products
        }

        with self.output().open("w") as outFile:
            outFile.write(json.dumps(output, indent=4, sort_keys=True))

    def output(self):
        return LocalTarget(os.path.join(self.stateLocation, "SearchForProductsFromList"))
    