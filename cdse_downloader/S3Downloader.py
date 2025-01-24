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
		# getter to see if routine downloaded
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
		# completion -> True
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

	def run_download_checker(self):
		# did download work? https://github.com/s3tools/s3cmd/blob/master/S3/ExitCodes.py
		# self._proc_returncode = 0
		if self._proc_returncode == 0:
			# download OK
			self._output_verbose = "DOWNLOAD OK"
			self._retry = False
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
            "onlineStatus": "Online",
            "productID": "S2A_MSIL1C_20230501T110621_N0509_R137_T30UYB_20230501T131147",
            "productPath": "/eodata/Sentinel-2/MSI/L1C/2023/05/01/S2A_MSIL1C_20230501T110621_N0509_R137_T30UYB_20230501T131147.SAFE"
        }
	obj = S3Downloader(entry, "./products", True)
	obj.set_s3_config("./s3cfg")
	obj.run_downloader(dry_run=False, force_dl=False)
	obj.run_download_checker()
	print("result:",obj.get_result())



