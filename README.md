```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

SPARK

```bash
cd terraform
terraform init
terraform apply
cd ..
gsutil cp gs://test-dataflow-eu/london_bikes/london_cycle_stations.csv gs://datalake_simple-python-dataflow/london_cycle_stations.csv
gsutil cp gs://test-dataflow-eu/london_bikes/hires/298JourneyDataExtract29Dec2021-04Jan2022.csv gs://datalake_simple-python-dataflow/hires/298JourneyDataExtract29Dec2021-04Jan2022.csv
```

```bash
cd terraform
terraform destroy
cd ..
```

![map](images/daily_agg_map_example.png)