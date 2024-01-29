import os
import hashlib
import subprocess
import xml.etree.ElementTree as ET
import zipfile
import logging
import shutil

log = logging.getLogger('luigi-interface')

class S3Downloader:

	_s3cmd = ""
	_url = ""
	_checksum = ""
	_uri = ""
	_target_location = ""
	_id = ""
	_proc_returncode = None
	_retry = False
	_blob = ""
	_output = None
	_output_verbose = ""
	_unzip = True

	# temporary verbose code list
	retryable = {
		1: "GENERAL",
		2: "SOME PARTS OF COMMAND SUCCEED, OTHERS FAILED",
		11: "400, 405, 411, 416, 417, 501: BAD REQUEST, 504: GATEWAY TIMEOUT",
		15: "503: SERVICE NOT AVAILABLE OR SLOW DOWN",
		65: "FAILED FILE TRANSFER, UPLOAD OR DOWNLOAD",
		70: "INTERNAL SOFTWARE ERROR",
		71: "SYSTEM ERROR",
		72: "OS ERROR",
		74: "I/O ERROR",
		75: "TEMPORARY FAILURE, RETRY LATER",
	}
	non_retryable = {
		10: "301: MOVED PERMANENTLY & 307: MOVED TEMP",
		12: "404: NOT FOUND",
		13: "409: CONFLICT (bucket error?)",
		14: "412: PRECONDITION FAILED",
		64: "COMMAND USED INCORRECTLY",
		77: "INSUFFICIENT PERMISSIONS TO PERFORM OPERATION ON S3",
		78: "CONFIG FILE ERROR",
		128: "_EX_SIGNAL",
		# _EX_SIGINT?
		130: "CTRL-C KeyboardInterrupt",
	}

	def __init__(self, in_entry, in_target_location=None, unzip=True):
		# set up locations and names
		self._url = in_entry['productPath']
		self._id = in_entry['productID']
		# get filename from productPath string
		loc = 0
		for ind, c in enumerate(self._url):
			if c == "/":
				loc = ind
		self._uri = in_entry['productPath'][loc:]
		self._target_location = in_target_location
		self._checksum = in_entry['productCheckSum'].upper()
		self._unzip = unzip

		# get config for s3cmd OR is it already set in environment
		self.s3_config = ""

	def get_retry_status(self):
		# getter to be used by luigi when routine fails
		return self._retry

	def get_verbose_exit_code(self):
		# get a verbose code error for logging
		return {"retcode": self._proc_returncode,
          		"error": self._output_verbose}

	def get_result(self):
		# getter to see if routine downloaded and verified checksum successfully
		return self._output

	def get_download_location(self):
		return self._target_location + self._uri

	def run_downloader(self, access_key_id=None, secret_key=None, dry_run=False, force_dl=False, skip_dl=False):
		# function to organise and be explicit to when running download routine
		self._url = self.format_url(self._url)
		self._s3cmd = self.build_s3_command(self._url, self._uri, self._target_location, self.s3_config, access_key_id, secret_key,  dry_run=dry_run, force_dl=force_dl)
		# skip download if downloaded already (for luigi)
		if not skip_dl:
			self._proc_returncode = self.get(self._s3cmd)
			self._output = True
		else:
			self._proc_returncode = 0
			self._output = True
		# print(f"\n\nrun downloader\n{self._proc_returncode}\n{self._output}\n\n")

	def set_s3_config(self, config_loc):
		self.s3_config = config_loc

	@staticmethod
	def get(in_s3cmd):
		# try download
		# completion -> _uri checksum -> True:False
		# fail -> False
		# function returns only the exit code, the deliverable is the downloads from s3cmd
		# todo: how to stop output from being sent to terminal, contain inside object
		# proc = subprocess.Popen(in_s3cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

		proc = subprocess.Popen(in_s3cmd)
		proc.wait()
		return proc.returncode


	@staticmethod
	def format_url(in_url):
		# remove any forward slashes if present
		if in_url[0] == "/":
			return in_url[1:]
		else:
			return in_url

	@staticmethod
	def build_s3_command(in_url, in_uri, in_target_location, s3_config, access_key_id, secret_key, dry_run=False, force_dl=False):
		"""
		:param in_url: path to the download on mundi web services
		:param in_uri: name of file/folder locally - used in conjunction with target location
		:param in_target_location: location for the files to be saved
		:param access_key_id: access key id
		:param secret_key: secret key
		:param dry_run: for debug and testing purposes
		:param force_dl: whether to force download overwrite of existing files
		:return: list of strings ready for subprocess Popen
		"""
		# make download folder
		try:
			if not os.path.exists(in_target_location):
				os.makedirs(in_target_location)
		except OSError as e:
			# todo : enter logging hook here
			print(e)
			exit()
		# if the download is a folder use case (.zip OR folder)
		try:
			if in_uri.find(".zip") == -1:
				if not os.path.exists(in_target_location+in_uri):
					os.makedirs(in_target_location+in_uri+"/")
		except OSError as e:
			# todo : enter logging hook here
			print(e)
			exit()
		# return completed command
		cmd = [
			f"s3cmd",
			"get",
   			'--rexclude="/$"',
			"--dry-run",
			"-r", "s3://" + in_url,
			in_target_location ,
			"-c", s3_config,
			"--force"
		]

		if access_key_id is not None:
			cmd.extend(
				[
					"--access_key", access_key_id,
					"--secret_key", secret_key
				]
			)

		# remove dry run
		if not dry_run:
			cmd.remove("--dry-run")
		# remove force dl
		if not force_dl:
			cmd.remove("--force")
		return cmd

		print(f"\n\nS3 cmd: \n {cmd}\n\n")

	@staticmethod
	def concatenate_file_paths(in_location, in_appendix):
		# combine the sub paths, remove './'
		fixed_appendix = in_appendix
		if in_appendix[0] == '.':
			fixed_appendix = fixed_appendix[1:]
		if fixed_appendix[0] == '/' and in_location[-1] == '/':
			fixed_appendix = fixed_appendix[1:]
		return in_location + fixed_appendix

	@staticmethod
	def get_manifest_checksum_list(in_location):
		# find the manifest file and create key:value mappings of file:checksum
		# for files with no checksum in manifest: value=False
		manifest_location = in_location + "/manifest.safe"
		manifest_list = []

		# todo add try block
		with open(manifest_location, 'r') as myfile:
			manifest = ET.fromstring(myfile.read().strip("\n"))
		# find all pairs
		for ii in manifest.find('dataObjectSection'):
			filepath = ""
			value = ""
			for jj in ii.find('byteStream'):
				# get location
				if jj.get("href"):
					filepath = jj.get("href")
				# get algorithm
				if jj.get("checksumName"):
					checkSumName = jj.get("checksumName").upper()
				# get checksum
				if jj.text:
					value = jj.text
					value = value.upper()
			# concat file location with full path
			filepath = S3Downloader.concatenate_file_paths(in_location, filepath)
			manifest_list.append({
				"filepath": filepath,
				"checksum": value,
				"checksumType": checkSumName
			})
		return manifest_list

	@staticmethod
	def checksum(in_location, in_uri, in_checksum, in_unzip):
		# if the download is a zip, unzip it and use the manifest file checksums
		total_path = in_location+in_uri
		# check if unzipped folder exists also
		unzipped = total_path[:-4]
		unzipped_bool = os.path.exists(unzipped)

		has_unzipped = False
		# if zip in file name AND folder same name as zip doesnt exist (from previous run)
		if not total_path.find(".zip") == -1 and not unzipped_bool:
			print("zip in file name AND folder same name as zip doesnt exist")

			# print("@@ unzipping @@")
			with zipfile.ZipFile(total_path,"r") as f:
				f.extractall(in_location+"/")
				f.close()
			# remove .SAFE extension if exists
			if os.path.exists(total_path[:-4]+".SAFE"):
				folder_name = total_path[:-4]
				os.rename(total_path[:-4]+".SAFE", total_path[:-4])
				total_path = folder_name

			# Indicate that we have just unzipped the file
			has_unzipped = True
			
		result = False
		# remove zip from total path
		if not total_path.find(".zip") == -1:
			total_path = total_path[:-4]
		# determine logic path for either .zip or SAFE folder
		if not total_path.find(".zip") == -1 and not unzipped_bool:
			# determine logic path for either .zip or SAFE folder 1
			# todo: remove this logic as you can't ever satisfy the condition for this if statement? The in_checksum is never used
			# result = S3Downloader.checksum_file(total_path, in_checksum)

			raise Exception("No handling for verifying checksum of zip files")
		else:
			# get file:hash from manifest, in_checksum is not used in this path
			manifest = S3Downloader.get_manifest_checksum_list(total_path)
			# iterate through all files and verify hash
			manifest_verified = {}
			for entry in manifest:
				# create file:hashstatus pairs
				manifest_verified.update({entry["filepath"]: S3Downloader.checksum_file(entry["filepath"], entry["checksum"], entry["checksumType"])})
			# todo: log files that have no hash provided?
			# if any hashes return false then verifying download == False
			result = False if False in manifest_verified.values() else True

		if not in_unzip and has_unzipped:
			# Remove unzipped folder here
			shutil.rmtree(unzipped, ignore_errors=True)
		elif in_unzip and has_unzipped:
			# Remove zips
			os.remove(total_path + ".zip")

		return result

	@staticmethod
	def checksum_file(in_file, in_checksum, checksumType):
		out = ""

		if checksumType == "MD5":
			out = hashlib.md5(open(in_file, 'rb').read()).hexdigest().upper()
		elif checksumType == "SHA3-256":
			out = hashlib.sha3_256(open(in_file, 'rb').read()).hexdigest().upper()
		else:
			raise Exception(f"Unsupported checksum algorithm {checksumType}")

		print(f"@@ {in_checksum} - {out} || {True if in_checksum == out else False} || {in_file}")
		return True if in_checksum == out else False

	def run_download_checker(self):
		# did download work? https://github.com/s3tools/s3cmd/blob/master/S3/ExitCodes.py
		# self._proc_returncode = 0
		if self._proc_returncode == 0:
			# download OK, verify hash
			self._output = self.checksum(self._target_location, self._uri, self._checksum, self._unzip)
			if self._output:
				self._output_verbose = "DOWNLOAD OK, CHECKSUM OK"
				self._retry = False
			else:
				self._output_verbose = "DOWNLOAD OK, CHECKSUM FAIL"
				self._retry = True
		elif self._proc_returncode in self.retryable:
			# retryable return codes
			self._retry = True
			self._output = False
			self._output_verbose = self.retryable[self._proc_returncode]
		elif self._proc_returncode in self.non_retryable:
			# non-retryable return codes
			self._retry = False
			self._output = False
			self._output_verbose = self.non_retryable[self._proc_returncode]
		else:
			log.warning("Unknown S3cmd return code: {}".format(self._proc_returncode))
			self._output = False
		pass


if __name__ == "__main__":

	entry = {
            "onlineStatus": "ONLINE",
            "productCheckSum": "",
            "productID": "S2A_MSIL1C_20230501T110621_N0509_R137_T30UYB_20230501T131147",
            "productPath": "/eodata/Sentinel-2/MSI/L1C/2023/05/01/S2A_MSIL1C_20230501T110621_N0509_R137_T30UYB_20230501T131147.SAFE"
        }
	obj = S3Downloader(entry, "./products", True)
	obj.set_s3_config("./s3cfg")
	obj.run_downloader(dry_run=False, force_dl=False)
	obj.run_download_checker()
	print("result:",obj.get_result())

	# entry = {
	# 	"productPath": "s1-l1-grd-2019-q1/2019/02/10/IW/DV/S1B_IW_GRDH_1SDV_20190210T062204_20190210T062229_014880_01BC5F_EC68",
	# 	"productID": "S1B_IW_GRDH_1SDV_20190210T062204_20190210T062229_014880_01BC5F_EC68",
	# 	"productCheckSum": "D0F850F14EB3550C7053E39C847CB089",
	# }
	# obj = S3Downloader(entry, "./downloads")
	# obj.run_downloader(dry_run=True, force_dl=True)
	# obj.run_download_checker()
	#
	# entry = {
	# 	"productPath": "s1-l1-grd/2018/01/01/IW/DV/S1A_IW_GRDH_1SDV_20180101T042052_20180101T042117_019956_021FB8_AE3C.zip",
	# 	"productID": "S1A_IW_GRDH_1SDV_20180101T042052_20180101T042117_019956_021FB8_AE3C",
	# 	"productCheckSum": "D0F850F14EB3550C7053E39C847CB089",
	# }
	# obj = S3Downloader(entry, "./downloads")
	# obj.run_downloader(dry_run=True, force_dl=True)
	# obj.run_download_checker()
	# print("result:", obj.get_result())

