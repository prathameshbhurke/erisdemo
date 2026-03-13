import great_expectations as gx
import redshift_connector

# Step 1: Connect to Redshift and pull data
conn = redshift_connector.connect(
    host='ecommerce-workgroup.680019129594.us-east-1.redshift-serverless.amazonaws.com',
    database='dev',
    port=5439,
    user='admin',
    password='DragonDaima2026'
)

cursor = conn.cursor()

# Step 2: Load fct_orders into a pandas dataframe
cursor.execute("SELECT * FROM fct_orders")
columns = [desc[0] for desc in cursor.description]
rows = cursor.fetchall()
conn.close()

import pandas as pd
df = pd.DataFrame(rows, columns=columns)
print(f"Loaded {len(df)} rows from fct_orders")
print(df.head())

# Step 3: Create GX context and data source
context = gx.get_context()
data_source = context.data_sources.add_pandas("my_pandas_source")
data_asset = data_source.add_dataframe_asset("fct_orders_asset")
batch_definition = data_asset.add_batch_definition_whole_dataframe("full_batch")
batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

# Step 4: Create expectation suite
suite = context.suites.add(gx.ExpectationSuite(name="fct_orders_suite"))

# Step 5: Define expectations (quality rules)
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="order_id"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_name"))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
    column="status",
    value_set=["completed", "cancelled", "pending"]
))
suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
    column="order_amount",
    min_value=0,
    max_value=10000
))
suite.add_expectation(gx.expectations.ExpectTableRowCountToBeBetween(
    min_value=1,
    max_value=10000
))

# Step 6: Run validation
validation_definition = context.validation_definitions.add(
    gx.ValidationDefinition(
        name="fct_orders_validation",
        data=batch_definition,
        suite=suite
    )
)

results = validation_definition.run(batch_parameters={"dataframe": df})

# Step 7: Print results
print("\n========== DATA QUALITY REPORT ==========")
print(f"Overall Status: {'✅ PASSED' if results.success else '❌ FAILED'}")
print(f"Total Checks: {len(results.results)}")
passed = sum(1 for r in results.results if r.success)
failed = sum(1 for r in results.results if not r.success)
print(f"Passed: {passed} | Failed: {failed}")
print("\nDetailed Results:")
for r in results.results:
    status = "✅" if r.success else "❌"
    print(f"  {status} {r.expectation_config.type}")
print("=========================================")
