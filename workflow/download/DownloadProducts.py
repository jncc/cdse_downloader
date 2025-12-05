import luigi
import json
import os
import logging

from workflow.download.DownloadProduct import DownloadProduct

log = logging.getLogger('luigi-interface')

class DownloadProducts(luigi.Task):
    stateLocation = luigi.Parameter()
    downloadLocation = luigi.Parameter()
    dryRun = luigi.BoolParameter(default=False)

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
        stateFilename = f"DownloadProducts.json"
        return luigi.LocalTarget(os.path.join(self.stateLocation, stateFilename))