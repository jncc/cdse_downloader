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

    def formatProductName(self, productName):
        # S1 example: S1A_IW_GRDH_1SDV_20251210T063215_20251210T063240_062249_07CB28_A406_COG
        # S2 example: S2A_MSIL1C_20250810T110701_N0511_R137_T30UXD_20250810T134221

        formattedName = productName.rstrip("\n").rstrip(".SAFE")

        if formattedName.startswith("S1") and not formattedName.endswith("_COG"):
            formattedName += "_COG"

        return formattedName

    def run(self):
        if self.envFilePath:
            load_dotenv(self.envFilePath)
        else:
            load_dotenv()

        productList = seq(open(self.productListFile)) \
                        .map(lambda line: self.formatProductName(str(line))) \
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
                                    "remotePath": r.assets["safe_manifest"].href
                                        .replace(f"s3://{bucketName}/", "")
                                        .replace("/manifest.safe", "")}) \
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
    