import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame
import concurrent.futures
import re

class GroupFilter:
      def __init__(self, name, filters):
        self.name = name
        self.filters = filters

def apply_group_filter(source_DyF, group):
    return(Filter.apply(frame = source_DyF, f = group.filters))

def threadedRoute(glue_ctx, source_DyF, group_filters) -> DynamicFrameCollection:
    dynamic_frames = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        future_to_filter = {executor.submit(apply_group_filter, source_DyF, gf): gf for gf in group_filters}
        for future in concurrent.futures.as_completed(future_to_filter):
            gf = future_to_filter[future]
            if future.exception() is not None:
                print('%r generated an exception: %s' % (gf, future.exception()))
            else:
                dynamic_frames[gf.name] = future.result()
    return DynamicFrameCollection(dynamic_frames, glue_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1722928828892 = glueContext.create_dynamic_frame.from_catalog(database="imdb-movie-db", table_name="imdb_movie_data_2023_csv", transformation_ctx="AmazonS3_node1722928828892")

# Script generated for node Data-quality-checks
Dataqualitychecks_node1722928971077_ruleset = """
    Rules = [
        ColumnExists "rating",
        ColumnValues "rating" between 7.5 and 9.4
    ]
"""

Dataqualitychecks_node1722928971077 = EvaluateDataQuality().process_rows(frame=AmazonS3_node1722928828892, ruleset=Dataqualitychecks_node1722928971077_ruleset, publishing_options={"dataQualityEvaluationContext": "Dataqualitychecks_node1722928971077", "enableDataQualityCloudWatchMetrics": True, "enableDataQualityResultsPublishing": True}, additional_options={"performanceTuning.caching":"CACHE_NOTHING"})

# Script generated for node ruleOutcomes
ruleOutcomes_node1722929030721 = SelectFromCollection.apply(dfc=Dataqualitychecks_node1722928971077, key="ruleOutcomes", transformation_ctx="ruleOutcomes_node1722929030721")

# Script generated for node rowLevelOutcomes
rowLevelOutcomes_node1722929027624 = SelectFromCollection.apply(dfc=Dataqualitychecks_node1722928971077, key="rowLevelOutcomes", transformation_ctx="rowLevelOutcomes_node1722929027624")

# Script generated for node Conditional Router
ConditionalRouter_node1722929148614 = threadedRoute(glueContext,
  source_DyF = rowLevelOutcomes_node1722929027624,
  group_filters = [GroupFilter(name = "Failed_records", filters = lambda row: (bool(re.match("Failed", row["DataQualityEvaluationResult"])))), GroupFilter(name = "default_group", filters = lambda row: (not(bool(re.match("Failed", row["DataQualityEvaluationResult"])))))])

# Script generated for node default_group
default_group_node1722929148784 = SelectFromCollection.apply(dfc=ConditionalRouter_node1722929148614, key="default_group", transformation_ctx="default_group_node1722929148784")

# Script generated for node Failed_records
Failed_records_node1722929148805 = SelectFromCollection.apply(dfc=ConditionalRouter_node1722929148614, key="Failed_records", transformation_ctx="Failed_records_node1722929148805")

# Script generated for node Change Schema
ChangeSchema_node1722929261775 = ApplyMapping.apply(frame=default_group_node1722929148784, mappings=[("snum", "long", "snum", "bigint"), ("movie name", "string", "movie_name", "string"), ("rating", "double", "rating", "decimal"), ("votes", "long", "votes", "bigint"), ("meta score", "string", "meta_score", "string"), ("genre", "string", "genre", "string"), ("pg rating", "string", "pg_rating", "varchar"), ("year", "long", "year", "bigint"), ("duration", "string", "duration", "string"), ("cast", "string", "movie_cast", "string"), ("director", "string", "director", "string")], transformation_ctx="ChangeSchema_node1722929261775")

# Script generated for node S3-rule-outcome
S3ruleoutcome_node1722929092122 = glueContext.write_dynamic_frame.from_options(frame=ruleOutcomes_node1722929030721, connection_type="s3", format="json", connection_options={"path": "s3://imdb-movie-data-analysis-by-shikha/rule_outcome/", "partitionKeys": []}, transformation_ctx="S3ruleoutcome_node1722929092122")

# Script generated for node Failed-records-in-S3
FailedrecordsinS3_node1722929207419 = glueContext.write_dynamic_frame.from_options(frame=Failed_records_node1722929148805, connection_type="s3", format="json", connection_options={"path": "s3://imdb-movie-data-analysis-by-shikha/bad_records/", "partitionKeys": []}, transformation_ctx="FailedrecordsinS3_node1722929207419")

# Script generated for node Redshift-table-load
Redshifttableload_node1722929292557 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1722929261775, connection_type="redshift", connection_options={"redshiftTmpDir": "s3://aws-glue-assets-009160057626-us-east-2/temporary/", "useConnectionProperties": "true", "dbtable": "movies.imdb_movies_rating", "connectionName": "Redshift connection", "preactions": "CREATE TABLE IF NOT EXISTS movies.imdb_movies_rating (snum BIGINT, movie_name VARCHAR, rating DECIMAL, votes BIGINT, meta_score VARCHAR, genre VARCHAR, pg_rating VARCHAR, year BIGINT, duration VARCHAR, movie_cast VARCHAR, director VARCHAR);"}, transformation_ctx="Redshifttableload_node1722929292557")

job.commit()