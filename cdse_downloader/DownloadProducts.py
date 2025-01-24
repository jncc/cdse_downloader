import luigi
import json
import os
import logging

from luigi.util import requires
from functional import seq
from datetime import datetime
from glob import glob
from .S3Downloader import S3Downloader
from .SearchForProducts import SearchForProducts
from .SearchForProductsFromList import SearchForProductsFromList

log = logging.getLogger('luigi-interface')

class DownloadProducts(luigi.Task):
    stateLocation = luigi.Parameter()
    s3ConfigLocation = luigi.Parameter()
    downloadLocation = luigi.Parameter()
    dryRun = luigi.BoolParameter(default=False)
    force = luigi.BoolParameter(default=False)
    unzip = luigi.BoolParameter(default=False)
    
    StateFileName = None

    def ensurePath(self, path):
        if not os.path.exists(path):
            os.makedirs(path)

    def run(self):
        if not os.path.isfile(self.s3ConfigLocation):
            raise Exception(f"S3 confing file does not exist: {self.s3ConfigLocation}")

        products = []
        with self.input().open('r') as productList:
            r = json.load(productList)
            products = r["productList"]

        self.ensurePath(self.downloadLocation)

        downloadTasks = []
        offlineCount = 0
        for product in products:

            if product["onlineStatus"] == "Online":

                downloadTasks.append(DownloadProduct(product=product
                    ,stateLocation=self.stateLocation
                    ,s3ConfigLocation=self.s3ConfigLocation
                    ,downloadLocation=self.downloadLocation
                    ,dryRun=self.dryRun
                    ,force=self.force
                    ,unzip=self.unzip))

            else: 
                offlineCount = offlineCount + 1
                log.error(f"Product ID {product['productID']} is offline")
        
        yield downloadTasks

        downloadedProducts = []
        output = []
        for t in downloadTasks:
            r = json.load(t.output().open("r"))
            
            output.append(r)

            if r["downloadResult"]:
                downloadedProducts.append(r["productID"])

        if len(downloadedProducts) != len(products):
            log.error("The following requested products were not downloaded")

            seq(products) \
                .difference(downloadedProducts) \
                .map(lambda x: log.error(x))

            if offlineCount > 0:
                log.error(f"This includes {offlineCount} that were offline")

            raise Exception("Some products were not downloaded")

        #If unzipping the files don't keep the zips.
        if self.unzip:
            searchPath = os.path.join(self.downloadLocation, "*.zip")
            for f in glob(searchPath):
                os.remove(f)

        with self.output().open("w") as outFile:
            result = {
                "productList" : output
            }
            outFile.write(json.dumps(result, indent=4, sort_keys=True))


    def output(self):
        return luigi.LocalTarget(os.path.join(self.stateLocation, self.StateFileName))


class DownloadProduct(luigi.Task):
    product = luigi.DictParameter();
    stateLocation = luigi.Parameter()
    s3ConfigLocation = luigi.Parameter();
    downloadLocation = luigi.Parameter();
    dryRun = luigi.BoolParameter(default=False);
    force = luigi.BoolParameter(default=False);
    unzip = luigi.BoolParameter(default=False)

    def run(self):
        downloadService = S3Downloader(self.product, self.downloadLocation, unzip=self.unzip)

        log.info("Download product %s" % self.product["productID"])
        
        # set config location
        downloadService.set_s3_config(self.s3ConfigLocation)
        downloadService.run_downloader(dry_run=self.dryRun, force_dl=self.force)
        if self.dryRun:
            successFlag = True
        else:
            downloadService.run_download_checker()
            # determine end status of task
            successFlag = downloadService.get_result()


        if not successFlag:
            msg = f"Download of {self.product['productID']} failed" 
            log.error(msg)
            detail = downloadService.get_verbose_exit_code()
            log.error(f"s3cmd retuned {detail['retcode']}, {detail['error']}")
            
            raise Exception(f"{msg}, see log for details")

        if not self.dryRun:
            localPath = os.path.join(self.downloadLocation, self.product["productID"])
        else:
            localPath = "n/a"
            
        with self.output().open("w") as outFile:
            result = {
                "productID" : self.product["productID"],
                "localPath" : localPath,
                "productPath" : self.product["productPath"],
                "downloadResult" : successFlag
            }
            outFile.write(json.dumps(result, indent=4, sort_keys=True))

    def output(self):
        statusFileName = f"{self.product['productID']}_download.json"
        return luigi.LocalTarget(os.path.join(self.stateLocation, statusFileName))

@requires(SearchForProducts)
class DownloadProductsByArea(DownloadProducts):
    StateFileName = "DownloadProductsByArea.json"
    
    def dummy_f(self):
        pass

@requires(SearchForProductsFromList)
class DownloadProductsFromList(DownloadProducts):
    StateFileName = "DownloadProductsFromList.json"
    
    def dummy_f(self):
        pass