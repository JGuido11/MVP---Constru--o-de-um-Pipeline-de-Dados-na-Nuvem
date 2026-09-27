"""Um único processo sequencial no Databricks coordena todas as camadas."""
from mobility.config import source_month


def parse_months(value):
    months = [source_month(item.strip()) for item in value.split(',')]
    if len(months) != len(set(months)):
        raise ValueError('Não repita meses em source_months.')
    return sorted(months)


def run_pipeline(spark, config, project_root, months, run_id, source_mode='volume'):
    from mobility.storage import DeltaStore
    from mobility.jobs import TripIngestionJob, TripPreparationJob
    from mobility.analytics import GoldBuilder
    from mobility.catalog import annotate_catalog

    if not months:
        raise ValueError('Informe pelo menos um mês.')
    for month in months:
        source_month(month)
    store = DeltaStore(spark, config)
    store.bootstrap()
    store.event(months[0], run_id, 'pipeline', 'STARTED')
    try:
        for month in months:
            TripIngestionJob(spark, config, store).run(month, run_id, source_mode=source_mode)
            TripPreparationJob(spark, config, store).run(month, run_id)
        result = GoldBuilder(spark, config, project_root).build(months)
        annotate_catalog(spark, config)
        store.event(months[0], run_id, 'pipeline', 'SUCCESS')
        return {**result, 'run_id': run_id}
    except Exception as error:
        store.event(months[0], run_id, 'pipeline', 'FAILED', type(error).__name__)
        raise
