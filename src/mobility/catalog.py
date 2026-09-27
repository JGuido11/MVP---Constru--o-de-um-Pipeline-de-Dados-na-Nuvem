"""Comentários de tabela e coluna no Unity Catalog a partir do catálogo versionado."""
import json
from pathlib import Path


def definitions():
    return json.loads(Path(__file__).with_name('catalog.json').read_text())


def field_definition(metadata, name):
    return next((info for key, info in metadata['fields'].items()
                 if key.casefold() == name.casefold()), None)


def annotate_catalog(spark, config):
    metadata = definitions()
    for qualified, table in metadata['tables'].items():
        layer, name = qualified.split('.')
        full = config.table(layer, name)
        if not spark.catalog.tableExists(full):
            continue
        # A view intermediária tem documentação no Markdown; comentários abaixo são de tabelas.
        if name == 'stg_trips':
            continue
        description = table['description'].replace("'", "''")
        spark.sql(f"COMMENT ON TABLE {full} IS '{description}'")
        for field in spark.table(full).schema.fields:
            info = field_definition(metadata, field.name)
            if info is None:
                raise ValueError(f'Atualize o catálogo para a coluna nova: {full}.{field.name}')
            comment = f"{info['description']} Domínio: {info['domain']}. Origem: {info['lineage']}."
            comment = comment.replace("'", "''")
            column = field.name.replace('`', '``')
            spark.sql(f"ALTER TABLE {full} ALTER COLUMN `{column}` COMMENT '{comment}'")
