# Executar no Databricks

## 1. Importar o repositório

Crie/abra uma pasta Git do repositório no Workspace e selecione a branch que contém esta versão. Copie o caminho completo dessa pasta, que contém `src`, `sql` e `notebooks`. Use-o no parâmetro `project_root` dos notebooks. Não use o caminho da subpasta `notebooks`.

Use um catálogo existente ao qual você tenha acesso; o padrão é `workspace`. O usuário precisa poder criar schemas, Volume e tabelas. O PySpark e o SQL são executados no compute do Databricks. Os notebooks não exigem instalação de pacotes adicionais.

## 2. Criar a estrutura e carregar os arquivos

Execute `notebooks/bootstrap.py` com `project_root`, `catalog=workspace` e `schema_prefix=mobility`. O bootstrap cria schemas e o Volume gerenciado `landing`.

No Catalog Explorer, abra o Volume `workspace.mobility_bronze.landing`. Crie as pastas `2026-01`, `2026-02` e `2026-03`. Faça upload do Parquet correspondente e uma cópia do CSV oficial de zonas em cada pasta:

| Pasta no Volume | Arquivos |
|---|---|
| `/Volumes/workspace/mobility_bronze/landing/2026-01/` | `yellow_tripdata_2026-01.parquet`, `taxi_zone_lookup.csv` |
| `/Volumes/workspace/mobility_bronze/landing/2026-02/` | `yellow_tripdata_2026-02.parquet`, `taxi_zone_lookup.csv` |
| `/Volumes/workspace/mobility_bronze/landing/2026-03/` | `yellow_tripdata_2026-03.parquet`, `taxi_zone_lookup.csv` |

Os quatro arquivos originais e suas URLs estão em [DATA_SOURCES.md](DATA_SOURCES.md). A cópia mensal do lookup é um snapshot de coleta. Não coloque arquivos nem credenciais no repositório.

Alternativa: `source_mode=download` baixa os arquivos no próprio Databricks. Se a rede bloquear o domínio da fonte, use o upload e `source_mode=volume`. Não contorne restrições do workspace.

## 3. Executar o pipeline único

Abra `notebooks/01_run_pipeline.py`, preencha os parâmetros e execute todas as células.

| Parâmetro | Valor/exemplo |
|---|---|
| `project_root` | Caminho completo da pasta Git no Workspace. |
| `catalog` | `workspace` ou outro catálogo autorizado. |
| `schema_prefix` | `mobility`; mantenha o mesmo prefixo em todas as etapas. |
| `source_months` | `2026-01,2026-02,2026-03`. |
| `source_mode` | `volume` para arquivos carregados; `download` para download no Databricks. |
| `run_id` | Vazio gera UUID; o Job usa seu identificador. |

Para automatizar, crie um Job com uma tarefa apontando para esse notebook, configure os mesmos parâmetros e limite a uma execução concorrente. Não crie tarefas concorrentes por mês. Alternativamente, o arquivo `databricks.yml` permite implantação por Databricks CLI com um perfil autenticado (`databricks bundle validate`, `databricks bundle deploy`, `databricks bundle run pipeline`). O host é fornecido pelo perfil, não fixado no repositório. O caminho por interface é suficiente para a entrega.

## 4. Conferir resultados

Após sucesso do Job, execute `06_quality.py`, `analysis.py` e `07_catalog.py`, sempre com o mesmo catálogo/prefixo. O notebook de análise exige os três meses e filtra o trimestre na comparação mensal. Capture as telas e preencha o [README](../README.md). O [roteiro de evidências](EVIDENCES.md) detalha o que registrar.

Para depuração manual, a sequência é bootstrap → `02_ingest_bronze.py` → `03_ingest_silver.py` (repetir para cada mês) → `05_build_gold.py`. A execução principal já faz essa sequência. Os notebooks avulsos não devem executar ao mesmo tempo que o Job.

## 5. Testes opcionais no Databricks

`acceptance.py` usa dados sintéticos e schemas `mobility_acceptance_*`. Testa rejeições, retenção de alertas, replay e recuperação. São objetos de teste, não evidência de análise dos táxis reais.

`04_test_reprocessing.py` usa os arquivos atuais do Volume e compara contagens/totais antes e depois. Execute isoladamente após Bronze/Silver bem-sucedidas e reconstrua a Gold em seguida. Ele não compara todas as células e não valida uma nova revisão da fonte. A tabela opcional `ops.reprocessing_evidence` registra as comparações.

## 6. Migração de instalações anteriores

As mudanças no Git não removem recursos já implantados. Pause agendamentos antigos que escrevam nos mesmos schemas, confirme que não há execução em andamento e passe a usar apenas o novo Job. Os nomes principais das tabelas foram preservados. Recursos externos antigos e arquivos já presentes no Workspace não são apagados por este código.

Se já existir um objeto `gold.stg_trips` do tipo tabela em vez de view, use um prefixo novo para a validação inicial ou ajuste esse objeto conscientemente. Não apague dados existentes apenas para testar. Para mudança de esquema incompatível, prefira um prefixo novo e reexecute com arquivos conhecidos.
