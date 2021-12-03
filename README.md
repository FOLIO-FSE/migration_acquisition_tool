# migration_acquisition_tool
Prerequisites for purchase Orders script
1. must include the okapi customer parameters in the ../runenv/okapi_customers.json
2. be sure that previusly you have worked the adquisitionsMapping.xsls file with the customer to including: orderType, orderFormat, paymentMethod, locations, etc.
3. with the tsv or csv file prepare the composite_purchase_order_mapping.json you can use the following tool: https://data-mapping-file-creator.folio.ebsco.com/data_mapping_creation .
NOTE: composite_purchase_orders_mapping is based on a json structure where the CSV headers are matched against the target fields in the FOLIO purchase orders and purchase orders line. To create a mapping file, use the web tool.

Running the scripts

Run the script using `main_AcqErm.py` and the following three positional arguments

```
positional arguments:
  client_name    Name of the client - must match name in okapi_customers.json
  script_to_run  Enter script to run Organizations=o | Orders=p | Licenses=l | Agreements=a | Notes=n
  download_ref   Do you want to download Acq Ref data: get_ref/no_ref

optional arguments:
  -h, --help     show this help message and exit
```

For example, to create Orders for the Alexandria library, without fetching reference data from FOLIO.

```
python main_AcqErm.py alexandria p no_ref
```
