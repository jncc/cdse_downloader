import luigi
import json
import os
import logging

from workflow.download.SearchForProducts import SearchForProducts
from luigi import LocalTarget
from luigi.util import requires

log = logging.getLogger('luigi-interface')

@requires(SearchForProducts)
class GenerateProductList(luigi.Task):
    stateLocation = luigi.Parameter()
    outputFile = luigi.Parameter()

    def run(self):
        products = []
        with self.input().open('r') as productList:
            products = json.load(productList)["productList"]

        with open(self.outputFile, "w") as outputFile:
            for product in products:
                outputFile.write(f"{product['productID']}\n")

        with self.output().open("w") as stateFile:
            result = {
                "productList" : products
            }
            stateFile.write(json.dumps(result, indent=4, sort_keys=True))

    def output(self):
        return LocalTarget(os.path.join(self.stateLocation, "GenerateProductList.json"))
    