import luigi
import json
import os
import logging

from datetime import datetime
from cdse_downloader.query_cdse import QueryCDSE
from luigi.parameter import MissingParameterException

log = logging.getLogger("luigi-interface")

class SearchForProducts(luigi.Task):
    stateLocation = luigi.Parameter()
    mission = luigi.ChoiceParameter(choices=["sentinel1", "sentinel2"], var_type=str)
    maxrecords = luigi.IntParameter(default=100)
    #defaults are just to negate these parameters not meaningful search values
    startDate = luigi.DateMinuteParameter(default=datetime.now())
    endDate = luigi.DateMinuteParameter(default=datetime.now())
    wkt = luigi.Parameter()
    s2CloudCover = luigi.IntParameter(default=0)
    onlineOnly = luigi.BoolParameter(default=True)
    s1RelativeOrbitNo = luigi.IntParameter(default=-1)
    productType = luigi.Parameter(default="")

    s1Polarisation = luigi.ChoiceParameter(default="", 
                                           choices=["", "VV&VH", "HH&HV", "VV", "HH"], 
                                           var_type=str)
    s1ProcessingLevel = luigi.Parameter(default="")
    s1SensorMode = luigi.Parameter(default="")
    
    params = {}
    
    def setS1Params(self):
        # add polarisation
        if self.s1Polarisation != "":
            self.params.update({"polarisation" : self.s1Polarisation})
                        
        if self.s1SensorMode != "":
            self.params.update({"sensorMode"  : self.s1SensorMode})
            
        if self.s1RelativeOrbitNo != -1:
            self.params.update({"relativeOrbitNumber" : self.s1RelativeOrbitNo})
            
        # only valid for s1 because product type determines processing level for s2
        if self.s1ProcessingLevel != "":
            self.params.update({"processingLevel": self.s1ProcessingLevel})
            
    def setS2Params(self):
        # add the cloud cover filter
        if self.s2CloudCover != 0:
            self.params.update({"cloudCover": f"[0,{self.s2CloudCover}]"})
            

    def run(self):
        
        self.params = {
            "geometry": self.wkt,
            "maxRecords": self.maxrecords
        }
    

        sat = 0

        if self.mission == 'sentinel1':
            sat = 1
            self.setS1Params()
        elif self.mission == 'sentinel2':
            sat = 2
            self.setS2Params()
            
    
        
        # add geo polygon
        if self.wkt == "":
            raise MissingParameterException("wkt must be specified")
        else:
            self.params.update({"geometry" : self.wkt})
        
        if self.onlineOnly:
            self.params.update({
                "status": "ONLINE"
            })
            
        # add date filter
        self.params.update({"startDate": self.startDate.strftime("%Y-%m-%dT%H:%M:%SZ"), 
                       "completionDate": self.endDate.strftime("%Y-%m-%dT%H:%M:%SZ")})
            
        # filter by product type
        if self.productType != "":
            self.params.update({"productType" : self.productType})
        

        cdse_obj = QueryCDSE(sat)
    
        features = cdse_obj.run_query(self.params, None)

        output = {"productList": features}

        with self.output().open("w") as outFile:
            outFile.write(json.dumps(output, indent=4, sort_keys=True))

    def output(self):
        return luigi.LocalTarget(os.path.join(self.stateLocation, "SearchForProducts.json"))

