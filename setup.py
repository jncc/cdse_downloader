import setuptools
 
setuptools.setup(
    name="cdse_downloader",
    version="0.0.2",
    author="JNCC",
    author_email="developers@jncc.gov.uk",
    description="A luigi workflow to download products from the Copernicus Data Space Ecosystem",
    long_description="""
        A Luigi workflow to download Sentinel 1 and 2 products from CDSE

        The workflow uses the Copernicus Data Space Ecosystem (CDSE) STAC API to get a subset of the Sentinel 1 and 2 raw products.

        [The CDSE data portal](https://dataspace.copernicus.eu/)

        [Luigi workflow](https://luigi.readthedocs.io/en/stable/index.html)
    """,
    long_description_content_type="text/markdown",
    url="https://github.com/jncc/cdse_downloader",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        'boto3',
        'luigi',
        'requests',
        'pyfunctional'
    ],
    python_requires='>=3.7',
)