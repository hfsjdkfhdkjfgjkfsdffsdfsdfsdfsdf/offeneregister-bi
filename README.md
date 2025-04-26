# OffeneRegister BI - reproducibility package

## Quick start

```bash
# 0) clone & virtual-env
git clone https://github.com/hfsjdkfhdkjfgjkfsdffsdfsdfsdfsdf/offeneregister-bi.git
cd offeneregister-bi
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# 1) (optional) decompress sample data
python src/bz2_copy.py data-sample/companies.jsonl.bz2 data-sample/companies.jsonl

# 2) run ETL
python src/transform_data.py \
      --input data-sample/companies.jsonl \
      --output data-sample/curated.parquet \
      --chunk 50000

# 3) open report/dashboard.pbix in Power BI Desktop (Windows) and click 'Refresh'
