import luigi
import json
import os
import logging

from pystac_client import Client
from functional import seq
from datetime import datetime
from luigi import LocalTarget
from luigi.util import requires

log = logging.getLogger('luigi-interface')

from dotenv import load_dotenv

load_dotenv()

class SearchForProducts(luigi.Task):
    stateLocation = luigi.Parameter()
    envFilePath = luigi.Parameter(default=None)
    startDate = luigi.DateMinuteParameter(default=datetime.now())
    endDate = luigi.DateMinuteParameter(default=datetime.now())
    geometry = luigi.Parameter()
    collection = luigi.ChoiceParameter(default="", 
                                        choices=["", "sentinel-1-grd", "sentinel-1-slc", "sentinel-2-l1c"],
                                        var_type=str)
    platform = luigi.Parameter(default=None)
    orbitDirection = luigi.ChoiceParameter(default="", 
                                        choices=["", "ascending", "descending"],
                                        var_type=str)
    relativeOrbitNumber = luigi.Parameter(default=None)

    s2CloudCover = luigi.Parameter(default=None)

    s1Polarisation = luigi.ChoiceParameter(default="", 
                                           choices=["", "VV", "VH"], 
                                           var_type=str)
    s1InstrumentMode = luigi.ChoiceParameter(default="IW", 
                                        choices=["", "IW", "EW"],
                                        var_type=str)

    def run(self):
        if self.envFilePath:
            load_dotenv(self.envFilePath)
        else:
            load_dotenv()

        stacUrl = os.getenv("STAC_API_URL")
        bucketName = os.getenv("AWS_BUCKET_NAME")

        dateRange = f"{self.startDate.strftime('%Y-%m-%dT%H:%M:%SZ')}/{self.endDate.strftime('%Y-%m-%dT%H:%M:%SZ')}"

        query = {}
        
        if self.platform:
            query["platform"] = {"eq": self.platform}
        
        if self.orbitDirection:
            query["sat:orbit_state"] = {"eq": self.orbitDirection}

        if self.relativeOrbitNumber:
            query["sat:relative_orbit"] = {"eq": self.relativeOrbitNumber}

        if self.s2CloudCover:
            query["eo:cloud_cover"] = {"lte": self.s2CloudCover}

        if self.s1InstrumentMode:
            query["sar:instrument_mode"] = {"eq": self.s1InstrumentMode}

        if self.s1Polarisation:
            query["sar:polarizations"] = {"eq": self.s1Polarisation}

        stacCatalog = Client.open(stacUrl)
        search = stacCatalog.search(
            collections=self.collection,
            datetime=dateRange,
            intersects=self.geometry,
            query=query
            )
        results = list(search.items())
        log.info(results)
        
        products = seq(results) \
                    .map(lambda r: {"productID": r.id,
                                    "remotePath": r.assets["safe_manifest"].href
                                        .replace(f"s3://{bucketName}/", "")
                                        .replace("/manifest.safe", "")}) \
                    .to_list()

        with self.output().open("w") as outFile:
            result = {
                "productList": products 
            }
            outFile.write(json.dumps(result, indent=4, sort_keys=True))

    def output(self):
        return LocalTarget(os.path.join(self.stateLocation, "SearchForProducts"))
    