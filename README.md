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
gsutil -m cp -r gs://test-dataflow-eu/london_bikes gs://BUCKET_NAME
bash jobs/run_spark.sh
```

```bash
cd terraform
terraform destroy
cd ..
```

![map](images/daily_agg_map_example.png)