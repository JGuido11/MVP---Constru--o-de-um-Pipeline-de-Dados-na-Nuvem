"""Dados sintéticos; testa SQL/Spark localmente, sem alegar execução Delta na nuvem."""
from pathlib import Path
import re
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from mobility.analytics import GoldBuilder, require_ready, MODEL_ORDER
from mobility.config import PipelineConfig
from mobility.pipeline import parse_months
ROOT = Path(__file__).resolve().parents[1]


class ReadinessTests(unittest.TestCase):
    def test_missing_or_failed_month_cannot_publish(self):
        for states in ([], [{'source_month':'2026-01','status':'FAILED'}],
                       [{'source_month':'2026-02','status':'SUCCESS'}],
                       [{'source_month':'2026-01','status':'SUCCESS'},
                        {'source_month':'2026-02','status':None}]):
            with self.subTest(states=states), self.assertRaises(ValueError):
                require_ready(states, ['2026-01'])

    def test_requested_months_are_valid_unique_and_sorted(self):
        self.assertEqual(parse_months('2026-02, 2026-01'), ['2026-01','2026-02'])
        for value in ('', '2026-01,2026-01', '2026-13'):
            with self.assertRaises(ValueError):
                parse_months(value)


class NativeAnalyticsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.temp = tempfile.TemporaryDirectory()
        cls.spark = (SparkSession.builder.master('local[2]').appName('mobility-native-tests')
                     .config('spark.sql.warehouse.dir', cls.temp.name)
                     .config('spark.sql.shuffle.partitions','2')
                     .config('spark.ui.enabled','false')
                     .config('spark.driver.bindAddress','127.0.0.1')
                     .config('spark.driver.host','127.0.0.1').getOrCreate())
        cls.spark.sparkContext.setLogLevel('ERROR')
        cls.config = PipelineConfig('spark_catalog','test_mobility')
        for layer in ('bronze','silver','gold','ops'):
            cls.spark.sql(f'CREATE DATABASE IF NOT EXISTS {cls.config.schema(layer)}')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.temp.cleanup()

    def test_native_models_preserve_multiplicity_and_reconcile(self):
        from mobility.validation import TripValidator
        from pyspark.sql import functions as F
        s, c = self.spark, self.config
        raw = s.sql('''SELECT cast(p AS timestamp_ntz) AS tpep_pickup_datetime,
            cast(d AS timestamp_ntz) AS tpep_dropoff_datetime,
            pu AS PULocationID, dest AS DOLocationID, 2.0 AS trip_distance,
            cast(f AS decimal(18,2)) AS fare_amount, cast(f AS decimal(18,2)) AS total_amount,
            '2026-01' AS source_month, 'fixture.parquet' AS source_filename,
            current_timestamp() AS ingested_at, 'run-test' AS ingestion_run_id
            FROM VALUES
            ('2026-01-10 10:00:00','2026-01-10 10:10:00',1,2,15),
            ('2026-01-10 10:00:00','2026-01-10 10:10:00',1,2,15),
            ('2026-01-10 10:00:00','2026-01-10 10:20:00',1,2,-5),
            (NULL,'2026-01-10 10:20:00',1,2,15)
            AS data(p,d,pu,dest,f)''')
        zones = s.sql('''SELECT * FROM VALUES
            (1,'Borough','A','Yellow','2026-01'),
            (2,'Borough','B','Yellow','2026-01')
            AS data(zone_id,borough,zone_name,service_zone,source_month)''')
        valid = TripValidator().validate(raw,zones).withColumn('preparation_run_id',F.lit('run-test'))
        def save(df,layer,name):
            df.write.mode('overwrite').format('parquet').saveAsTable(c.table(layer,name))
        save(raw,'bronze','yellow_trips')
        save(valid.where('size(rejection_reasons)=0'),'silver','trips')
        save(valid.where('size(rejection_reasons)>0'),'silver','rejected_trips')
        save(zones,'silver','taxi_zones')
        save(s.sql("SELECT '2026-01' AS source_month, 'SUCCESS' AS status, 'run-test' AS run_id, 4L AS input_count, 3L AS accepted_count, 1L AS rejected_count"),'ops','month_status')

        class LocalSQL:
            # O SQL de transformação é o de produção; só a persistência Delta é substituída.
            def table(self,name): return s.table(name)
            def sql(self,query):
                match=re.match(r'CREATE OR REPLACE TABLE (\S+) USING DELTA AS (.*)',query,re.S)
                if match:
                    frame=s.sql(match[2])
                    frame.write.mode('overwrite').format('parquet').saveAsTable(match[1])
                    return s.table(match[1])
                return s.sql(query)
        builder=GoldBuilder(LocalSQL(),c,ROOT)
        result=builder.build(['2026-01'])
        self.assertEqual(result['status'],'SUCCESS')
        self.assertEqual(s.table(c.table('gold','fct_trips')).count(),3)
        monthly=s.table(c.table('gold','mart_monthly_activity')).first()
        self.assertEqual(monthly.trip_count,3)
        self.assertEqual(float(monthly.recorded_fare_amount),25.0)
        self.assertEqual(monthly.unusual_trip_count,1)
        # Reexecução Gold mantém multiplicidade e os totais.
        builder.build(['2026-01'])
        self.assertEqual(s.table(c.table('gold','fct_trips')).count(),3)
        # Uma geração divergente impede reconstruir a Gold.
        s.sql(f"CREATE OR REPLACE TEMP VIEW bad_status AS SELECT source_month,status,'other-run' AS run_id,input_count,accepted_count,rejected_count FROM {c.table('ops','month_status')}")
        class BadGeneration(LocalSQL):
            def sql(self,query):
                return super().sql(query.replace(c.table('ops','month_status'),'bad_status'))
        with self.assertRaisesRegex(ValueError,'preparation_generation_matches'):
            GoldBuilder(BadGeneration(),c,ROOT).build(['2026-01'])
        # Dimensão duplicada é barrada pelo teste de chaves.
        dim=s.table(c.table('gold','dim_zones'))
        dim.unionAll(dim).createOrReplaceTempView('bad_dim')
        class BadDimension(LocalSQL):
            def sql(self,query):
                return super().sql(query.replace(c.table('gold','dim_zones'),'bad_dim'))
        with self.assertRaisesRegex(ValueError,'dimension_keys'):
            GoldBuilder(BadDimension(),c,ROOT).check('dimension_keys')

if __name__ == '__main__':
    unittest.main()
