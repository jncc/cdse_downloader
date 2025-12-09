import luigi
import json
import os
import logging

from workflow.download.DownloadProduct import DownloadProduct
from workflow.download.SearchForProducts import SearchForProducts
from workflow.download.SearchForProductsFromList import SearchForProductsFromList
from luigi import LocalTarget
from luigi.util import requires

log = logging.getLogger('luigi-interface')

class DownloadProducts(luigi.Task):
    stateLocation = luigi.Parameter()
    downloadLocation = luigi.Parameter()
    dryRun = luigi.BoolParameter(default=False)

    _stateFileName = ""

    def run(self):
        products = []
        with self.input().open('r') as productList:
            products = json.load(productList)["productList"]

        tasks = []
        for product in products:
            tasks.append(DownloadProduct(
                remoteProductPath=product["remotePath"],
                stateLocation=self.stateLocation,
                downloadLocation=self.downloadLocation,
                dryRun=self.dryRun))

        yield tasks

        output = []
        for task in tasks:
            taskOutput = json.load(task.output().open("r"))
            output.append(taskOutput)

        with self.output().open("w") as outFile:
            result = {
                "productList" : output
            }
            outFile.write(json.dumps(result, indent=4, sort_keys=True))

    def output(self):
        return LocalTarget(os.path.join(self.stateLocation, self._stateFileName))
    
@requires(SearchForProducts)
class DownloadProductsByArea(DownloadProducts):
    _stateFileName = "DownloadProductsByArea.json"
    
    def nullFunction(self):
        pass

@requires(SearchForProductsFromList)
class DownloadProductsFromList(DownloadProducts):
    _stateFileName = "DownloadProductsFromList.json"
    
    def nullFunction(self):
        pass