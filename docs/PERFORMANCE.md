# Spark Performance Tuning

## Monitoring Tools

### Spark UI
Access at `http://<driver-node>:4040`

Key tabs:
- **Jobs**: Overall job progress
- **Stages**: Task execution details
- **Storage**: Cached RDDs/DataFrames
- **SQL**: Query plans and metrics

### Logging
```python
# Set log level programmatically
spark.sparkContext.setLogLevel("WARN")
```

Benchmarking
Timing Execution
```python
import time

start = time.time()
# Your Spark operation
df.count()
end = time.time()
print(f"Execution time: {end-start:.2f} seconds")
```

Comparing Approaches
```python
%%timeit -n 3 -r 1
# DataFrame API
df.groupBy("category").agg({"price":"avg"}).collect()

%%timeit -n 3 -r 1
# SQL equivalent
spark.sql("SELECT category, AVG(price) FROM table GROUP BY category").collect()
```
Common Bottlenecks
Data Skew
```python
# Identify skew
df.groupBy("key").count().orderBy("count", ascending=False).show()

# Solution: Salting technique
from pyspark.sql.functions import concat, lit, rand

df = df.withColumn("salted_key", 
                  concat("key", lit("_"), (rand() * 10).cast("int")))
```
Shuffle Overhead
```python
# Check shuffle size
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```
Performance Metrics
Key metrics to monitor:

GC Time: Should be < 10% of task time

Shuffle Read/Write: Large values indicate expensive operations

Task Duration: Uneven times suggest skew

```python
# Access metrics programmatically
metrics = spark.sparkContext.statusTracker().getJobIdsForGroup()
```

Best Practices

Do's
- ✅ Cache frequently used DataFrames
- ✅ Use broadcast joins for small tables
- ✅ Partition data before multiple operations
- ✅ Use columnar file formats (Parquet/ORC)

Don'ts
- ❌ Collect large datasets to driver
- ❌ Use Python UDFs when native functions exist
- ❌ Keep default partition settings for large datasets
- ❌ Cache unnecessarily

Configuration Checklist
```python
# Recommended settings for performance
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10MB
```