# migration_acquisition_tool

Run the script using `main_AcqErm.py` and the following three positional arguments

```
positional arguments:
  client_name    Name of the client - must match name in okapi_customers.json
  script_to_run  Enter script to run Organizations=o | Orders=p | Licenses=l | Agreements=a | Notes=n
  download_ref   Do you want to download Acq Ref data: get_ref/no_ref

optional arguments:
  -h, --help     show this help message and exit
```

For example, to create Orders for the Alexandria library, without fetching reference data from FOLIO

```
python main_AcqErm.py alexandria p no_ref
```
