import luigi
import json
import os
import logging

from luigi.util import requires
from datetime import datetime
from cdse_downloader.SearchForProducts import SearchForProducts
from cdse_downloader.SearchForProductsFromList import SearchForProductsFromList
from cdse_downloader.DownloadProducts import DownloadProductsByArea, DownloadProductsFromList
from cdse_downloader.report_generator import ReportGenerator

log = logging.getLogger('luigi-interface')

class GenerateReport(luigi.Task):
    stateLocation = luigi.Parameter()
    
    mission = luigi.Parameter()
    reportFile = luigi.Parameter()
    dbFile = luigi.OptionalParameter(default=None)
    dbConnectionTimeout = luigi.IntParameter(default=60000)

    def run(self):
        with self.input().open('r') as productList:
            data = json.load(productList)

            rg = ReportGenerator(self.dbConnectionTimeout)

            rg.makeReport(self.mission, data, self.reportFile, self.dbFile)

            log.info("Generated download report: %s", self.reportFile)

    def output(self):

        return luigi.LocalTarget(self.reportFile)

@requires(DownloadProductsByArea)
class DownloadByAreaWithReport(GenerateReport):
    def dummy_f(self):
        pass

@requires(DownloadProductsFromList)
class DownloadFromListWithReport(GenerateReport):
    def dummy_f(self):
        pass

@requires(SearchForProducts)
class GenerateReportByArea(GenerateReport):
    def dummy_f(self):
        pass

@requires(SearchForProductsFromList)
class GenerateReportFromList(GenerateReport):
    def dummy_f(self):
        pass
