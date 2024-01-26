python query_cdse.py \
	--startdate 2023-05-01T00:00:00Z \
	--completiondate 2023-05-05T23:59:59Z \
	--sat 2 \
	--wkt wkt.txt \
	--rel_orbit_deny_list "80,123" \
	--cloud "0,95" \
	--csv output.txt
	
	
s3cmd -c ~/.s3cfg -r get s3://eodata/Sentinel-2/MSI/L1C/2023/05/01/S2A_MSIL1C_20230501T110621_N0509_R137_T30UYB_20230501T131147.SAFE/ 


"features"

productId = properties.title - remove .SAFE from the end
productPath = properties.productIdentifier 
onlineStatus = properties.status





Some products:
s3cmd -c ~/.s3cfg -r get s3://eodata/Sentinel-2/MSI/L1C/2023/05/26/S2B_MSIL1C_20230526T110629_N0509_R137_T30UXC_20230526T114909.SAFE

s3cmd -c ~/.s3cfg -r get s3://eodata/Sentinel-2/MSI/L1C/2023/05/26/S2B_MSIL1C_20230526T110629_N0509_R137_T30UXD_20230526T114909.SAFE

s3cmd -c ~/.s3cfg -r get s3://eodata/Sentinel-2/MSI/L1C/2023/05/26/S2B_MSIL1C_20230526T110629_N0509_R137_T31UCU_20230526T114909.SAFE

s3cmd -c ~/.s3cfg -r get s3://eodata/Sentinel-2/MSI/L1C/2023/05/26/S2B_MSIL1C_20230526T110629_N0509_R137_T30UYC_20230526T114909.SAFE

s3cmd -c ~/.s3cfg -r get s3://eodata/Sentinel-2/MSI/L1C/2023/05/26/S2B_MSIL1C_20230526T110629_N0509_R137_T30UWC_20230526T114909.SAFE

s3cmd -c ~/.s3cfg -r get s3://eodata/Sentinel-2/MSI/L1C/2023/05/26/S2B_MSIL1C_20230526T110629_N0509_R137_T30UWD_20230526T114909.SAFE


python query_cdse.py \
        --startdate 2022-05-01T00:00:00Z \
        --completiondate 2023-05-05T23:59:59Z \
        --sat 2 \
        --wkt "POLYGON((-1.213619303149454 50.84007494841458,0.0892367134668721 50.84007494841458,0.0892367134668721 51.82135325292194,-1.213619303149454 51.82135325292194,-1.213619303149454 50.84007494841458))" \
        --cloud "0,95" \
        --csv output.txt \
        --rel_orbit_deny_list "94,137" \
        --relativeorbit "94,137"

end points = Sentinel1, Sentinel2



LUIGI_CONFIG_PATH='path' PYTHONPATH='path' luigi --module cdse_downloader DownloadProductsByArea --startDate=2022-05-01T0000 --endDate=2023-05-05T2359 --local-scheduler