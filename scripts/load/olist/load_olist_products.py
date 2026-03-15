import redshift_connector
import os
from dotenv import load_dotenv

load_dotenv()

conn = redshift_connector.connect(
    host=os.getenv("REDSHIFT_HOST"),
    database=os.getenv("REDSHIFT_DB"),
    port=int(os.getenv("REDSHIFT_PORT", 5439)),
    user=os.getenv("REDSHIFT_USER"),
    password=os.getenv("REDSHIFT_PASSWORD")
)
conn.autocommit = True
cursor = conn.cursor()

S3_BUCKET = "s3://ecommerce-airbyte-staging/olist"
IAM_ROLE = "arn:aws:iam::680019129594:role/redshift-s3-role"

print("Loading raw_olist_products...")
cursor.execute(f"""
    COPY raw_olist_products
    FROM '{S3_BUCKET}/olist_products_dataset.csv'
    IAM_ROLE '{IAM_ROLE}'
    CSV
    IGNOREHEADER 1
    BLANKSASNULL
    EMPTYASNULL
""")
print("✅ raw_olist_products loaded!")
conn.close()
