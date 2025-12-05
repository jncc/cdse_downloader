import luigi
import json
import os
import logging
import boto3

from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger('luigi-interface')

class DownloadProduct(luigi.Task):
    remoteProductPath = luigi.Parameter()
    stateLocation = luigi.Parameter()
    downloadLocation = luigi.Parameter()
    dryRun = luigi.BoolParameter(default=False)

    def getLocalPath(self, objectKey):
        remoteDirectory = os.path.dirname(self.remoteProductPath)
        relativePath = objectKey.replace(remoteDirectory, "").strip("/")

        return os.path.join(self.downloadLocation, relativePath)

    def run(self):
        productID = os.path.basename(self.remoteProductPath)

        s3 = boto3.resource("s3")
        bucket = s3.Bucket(os.getenv("AWS_BUCKET_NAME"))
        files = bucket.objects.filter(Prefix=self.remoteProductPath)

        if not list(files):
            raise FileNotFoundError(f"Could not find any files for {productID}")

        log.info(f"Download product {productID}")
        if not self.dryRun:
            for file in files:
                localPath = self.getLocalPath(file.key)
                
                os.makedirs(os.path.dirname(localPath), exist_ok=True)
                if not os.path.isdir(file.key):
                    bucket.download_file(file.key, localPath)
        else:
            log.info("--dryRun mode enabled, skipping download")

        with self.output().open("w") as outFile:
            result = {
                "productID" : productID,
                "localPath" : os.path.join(self.downloadLocation, productID)
            }
            outFile.write(json.dumps(result, indent=4, sort_keys=True))

    def output(self):
        productID = os.path.dirname(self.remoteProductPath).replace(".SAFE", "")
        stateFilename = f"DownloadProduct_{productID}.json"
        return luigi.LocalTarget(os.path.join(self.stateLocation, stateFilename))