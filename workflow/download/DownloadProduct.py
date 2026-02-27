import luigi
import json
import os
import logging
import boto3
import time

from botocore.exceptions import ClientError
from dotenv import load_dotenv

log = logging.getLogger('luigi-interface')

class DownloadProduct(luigi.Task):
    retry_count = 3 # luigi task retries

    remoteProductPath = luigi.Parameter()
    stateLocation = luigi.Parameter()
    downloadLocation = luigi.Parameter()
    dryRun = luigi.BoolParameter(default=False)
    envFilePath = luigi.Parameter(default=None)

    def getLocalPath(self, objectKey):
        remoteDirectory = os.path.dirname(self.remoteProductPath)
        relativePath = objectKey.replace(remoteDirectory, "").strip("/")

        return os.path.join(self.downloadLocation, relativePath)
    
    def download_with_retries(self, bucket, key, localPath, max_retries=5, wait=5):
        attempt = 0
        while attempt < max_retries:
            try:
                bucket.download_file(key, localPath)
                return
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "")
                if error_code == "403":
                    attempt += 1
                    log.warning(f"403 Forbidden for {key}, retry {attempt}/{max_retries} after {wait}s")
                    time.sleep(wait)
                else:
                    raise
        raise Exception(f"Failed to download {key} after {max_retries} retries due to repeated 403 errors.")
        
    def run(self):
        if self.envFilePath:
            load_dotenv(self.envFilePath)
        else:
            load_dotenv()

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
                    self.download_with_retries(bucket, file.key, localPath)
        else:
            log.info("--dryRun mode enabled, skipping download")

        with self.output().open("w") as outFile:
            result = {
                "productID" : productID,
                "localPath" : os.path.join(self.downloadLocation, productID)
            }
            outFile.write(json.dumps(result, indent=4, sort_keys=True))

    def output(self):
        productID = os.path.basename(self.remoteProductPath).replace(".SAFE", "")
        stateFilename = f"DownloadProduct_{productID}.json"
        return luigi.LocalTarget(os.path.join(self.stateLocation, stateFilename))