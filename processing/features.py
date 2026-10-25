
"""Derived traffic metrics; byte counts mean payload bytes, not wire bytes."""

from functools import reduce

from pyspark.sql import functions as F


def add_features(frame):
    sources = ("total_fwd_packets", "total_backward_packets",
               "total_length_of_fwd_packets", "total_length_of_bwd_packets")
    types = dict(frame.dtypes)
    if any(types.get(n) != "bigint" for n in sources):
        raise ValueError("Feature sources must be present with contract LongType")

    def total(left, right):
        # Decimal avoids signed Long overflow, including when ANSI is enabled.
        a, b = F.col(left), F.col(right)
        return F.when(a.isNotNull() & b.isNotNull() & (a >= 0) & (b >= 0),
                      (a.cast("decimal(20,0)") + b.cast("decimal(20,0)")).cast("decimal(20,0)"))

    result = (frame.withColumn("total_packets", total(*sources[:2]))
              .withColumn("total_bytes", total(*sources[2:])))
    for name, source in (("fwd_packet_ratio", sources[0]), ("bwd_packet_ratio", sources[1])):
        result = result.withColumn(name, F.when(F.col("total_packets") > 0,
                                               F.col(source).cast("double") / F.col("total_packets").cast("double")))
    return result


def aggregate_flows(annotated):
    """Counts/ratios use all records; volume and duration use VALID only.

    One table contains batch, label, source_file and source_file+label groups.
    grouping distinguishes a true NULL label from a rolled-up dimension.
    """
    frame = add_features(annotated)
    valid = F.col("quality_status") == "VALID"
    measures = [F.count("*").alias("flow_count")]
    for status in ("VALID", "QUARANTINED", "REJECTED"):
        measures.append(F.coalesce(F.sum(F.when(F.col("quality_status") == status, 1).otherwise(0)), F.lit(0))
                        .alias(status.lower() + "_records"))
    measures.extend([
        F.coalesce(F.sum(F.when(valid, F.col("total_packets"))), F.lit(0)).alias("total_packets_sum"),
        F.coalesce(F.sum(F.when(valid, F.col("total_bytes"))), F.lit(0)).alias("total_bytes_sum"),
        F.avg(F.when(valid, F.col("flow_duration"))).alias("avg_flow_duration"),
    ])
    frames = []
    for grouping, keys in (("batch", []), ("label", ["label"]), ("source_file", ["source_file"]),
                           ("source_file_label", ["source_file", "label"])):
        group = frame.groupBy(*keys).agg(*measures).withColumn("grouping", F.lit(grouping))
        for name in ("source_file", "label"):
            if name not in keys:
                group = group.withColumn(name, F.lit(None).cast("string"))
        group = (group.withColumn("valid_ratio", F.when(F.col("flow_count") > 0,
                                                       F.col("valid_records") / F.col("flow_count")))
                 .withColumn("error_ratio", F.when(F.col("flow_count") > 0,
                     (F.col("quarantined_records") + F.col("rejected_records")) / F.col("flow_count"))))
        frames.append(group)
    return reduce(lambda a, b: a.unionByName(b), frames)
