# Spark Optimization Guide


## Data Partitioning

### Optimal Partition Count
```python
# Recommended: 2-4 partitions per CPU core
spark.conf.set("spark.sql.shuffle.partitions", "8")  # For local 4-core machine
```

Partitioning Strategies
Repartition by column:

```python
df.repartition(4, "region_column")
```

Coalesce (reduce partitions without shuffle):

```python
df.coalesce(2)
```

Memory Management
Storage Levels
```python
from pyspark import StorageLevel
```

## Persistence options:
```python
df.persist(StorageLevel.MEMORY_ONLY)           # Fastest, uses only memory
df.persist(StorageLevel.MEMORY_AND_DISK)       # Spills to disk if needed
df.persist(StorageLevel.DISK_ONLY)             # Disk only
```


## Recommended settings:
```python
spark.executor.memory=4g
spark.driver.memory=2g
spark.memory.fraction=0.6
```

## Execution Plan Optimization
Predicate Pushdown
```python
# Spark will automatically push filters down
df.filter("date > '2023-01-01'").select("id", "amount").explain()
```

## Join Strategies
```python
# Broadcast join for small tables (<10MB)
df1.join(broadcast(df2), "key")

# Sort-merge join for large tables
spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")
```

```python
# Writing optimized files
df.write.parquet("path/to/output", mode="overwrite", compression="snappy")
```

## Broadcast Variables
```python
small_df = spark.table("small_table")
broadcast_df = broadcast(small_df)

large_df.join(broadcast_df, "join_key")
```