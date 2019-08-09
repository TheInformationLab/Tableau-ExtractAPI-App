# Tableau ExtractAPI App
 
Using the latest Hyper API can dramatically increase the speed of generating Hyper extracts from CSV files. This python app is based on the Tableau Hyper API for Python version 0.0.1190 and allows for CSV files (or any tab or character seperated files) to be parsed into Hyper extracts.

## Installation

[Download the binary](https://github.com/TheInformationLab/Tableau-ExtractAPI-App/releases/tag/2.0.0) matching your OS

### OS Notes

#### Mac OS
Once downloaded you'll need to give the binary execute permissions using `chmod +x ./extract`

If you want to be able to run the extract command from any directory as a global command then simply copy the binary to your `/usr/local/bin/` directory (`cp extract /usr/local/bin/extract` or if you want the global command to be, for instance, `hyperextract` then the copy command would be `cp extract /usr/local/bin/hyperextract`)

## Usage

For a walkthrough on using the app check out [this blog post](https://theinformationlab.co.uk/2019/08/08/extracting-before-you-can-say-comma-separated-variable/)

```
usage: extract [-h] -s SCHEMA [-a ACCESSKEY] [-k SECRETKEY] [-b BUCKET]
               [-p PATH] [-o OUTPUT] [-d DELIMITER] [-i] [-w] [-l] [-t]

Tableau CSV to Hyper Parser by Craig Bloodworth, The Information Lab

optional arguments:
  -h, --help            show this help message and exit
  -s SCHEMA, --schema SCHEMA
                        JSON file containing the SCHEMA of the source CSV file.
                        (required)
  -a ACCESSKEY, --accesskey ACCESSKEY
                        AWS Access Key with permission to read the S3 bucket.
                        (optional)
  -k SECRETKEY, --secretkey SECRETKEY
                        AWS Secret Key belonging to the access key.
                        (optional)
  -b BUCKET, --bucket BUCKET
                        AWS S3 Bucket containing the source CSV file.
                        (optional)
  -p PATH, --path PATH  Path to the CSV file if using subfolders. No trailing slash!
                        (optional, e.g. /folder/subfolder)
  -o OUTPUT, --output OUTPUT
                        Filename of the extract to be created or extended.
                        (optional, default='output.hyper')
  -d DELIMITER, --delimiter DELIMITER
                        Specify the CSV delimiter character. Use \\t for tab delimited.
                        (optional, default=',')
  -i, --ignoreheader    Skip first line of each CSV file as they contain a header line.
                        (optional, default='False')
  -w, --overwrite       Overwrite existing hyper file.
                        (optional, default='False')
  -l, --localread       Skip S3 download and read local cached file.
                        (optional, default='False')
  -t, --temptable       Load first into a temp table.
                        (optional, default='False')
```
