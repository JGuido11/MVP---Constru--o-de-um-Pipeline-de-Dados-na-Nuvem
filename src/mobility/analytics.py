"""Construção da Gold com SQL nativo; falhas interrompem a publicação."""
from pathlib import Path

from mobility.config import source_month

MODEL_ORDER = (
    'dim_zones', 'stg_trips', 'fct_trips', 'mart_pickup_activity',
    'mart_monthly_activity', 'mart_route_duration',
)
PRE_CHECKS = ('months_are_ready', 'preparation_generation_matches',
              'rejections_have_reasons', 'source_counts_reconcile')


def require_ready(states, requested_months):
    requested = {source_month(month) for month in requested_months}
    if not requested:
        raise ValueError('Informe pelo menos um mês.')
    if any(row['status'] != 'SUCCESS' for row in states):
        raise ValueError('Todos os meses carregados devem ter preparação SUCCESS.')
    missing = requested - {row['source_month'] for row in states}
    if missing:
        raise ValueError(f'Meses ainda não preparados: {sorted(missing)}')


class GoldBuilder:
    def __init__(self, spark, config, project_root):
        self.spark = spark
        self.config = config
        self.sql_root = Path(project_root) / 'sql'

    def query(self, folder, name):
        template = (self.sql_root / folder / f'{name}.sql').read_text()
        return template.format(**{layer: self.config.schema(layer)
                                  for layer in ('bronze', 'silver', 'gold', 'ops')})

    def check(self, name):
        if self.spark.sql(self.query('checks', name)).limit(1).count():
            raise ValueError(f'Falha na validação: {name}')

    def build(self, requested_months):
        states = [row.asDict() for row in self.spark.table(
            self.config.table('ops', 'month_status')).select('source_month', 'status').collect()]
        require_ready(states, requested_months)
        for name in PRE_CHECKS:
            self.check(name)
        for name in MODEL_ORDER:
            target = self.config.table('gold', name)
            kind = 'VIEW' if name == 'stg_trips' else 'TABLE'
            using = '' if kind == 'VIEW' else ' USING DELTA'
            self.spark.sql(f'CREATE OR REPLACE {kind} {target}{using} AS {self.query("gold", name)}')
        for name in ('dimension_keys', 'fact_relationships', 'gold_generation_matches', 'valid_journeys', 'aggregate_totals_reconcile'):
            self.check(name)
        return {'status': 'SUCCESS', 'models': list(MODEL_ORDER), 'months': list(requested_months)}
