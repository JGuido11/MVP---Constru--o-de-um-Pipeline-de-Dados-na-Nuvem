# Arquitetura do pipeline

## Plataforma e fluxo

Todo processamento ocorre no Databricks. Arquivos ficam em um Volume gerenciado e as tabelas usam Delta Lake, organizadas no Unity Catalog. Um Job executa `notebooks/01_run_pipeline.py` com uma única tarefa. Dentro dela, os meses são processados sequencialmente e a Gold é reconstruída ao final.

| Componente | Responsabilidade |
|---|---|
| `src/mobility/source.py` | Download opcional dos arquivos oficiais para o Volume. |
| `src/mobility/storage.py` | Schemas, Volume, gravação Delta por mês e eventos. |
| `src/mobility/jobs.py` | Ingestão Bronze e preparação Silver. |
| `src/mobility/validation.py` | Tipagem, rejeições e alertas. |
| `src/mobility/analytics.py` | SQL da Gold e validações antes/depois. |
| `src/mobility/pipeline.py` | Sequência completa e evento final de sucesso/falha. |
| `src/mobility/catalog.py` | Comentários de tabelas e colunas. |
| `notebooks/analysis.py`, `06_quality.py`, `07_catalog.py` | Análises, avaliação de qualidade e inspeção do catálogo. |

## Modelagem

Bronze preserva os campos da fonte. Silver mantém registros normalizados, separa rejeitados e cria o lookup tipado. Gold contém uma dimensão mensal de zonas, uma view de preparação, uma fato e três agregações.

| Relação | Cardinalidade e chave |
|---|---|
| `silver.taxi_zones` → `gold.dim_zones` | Uma zona por `(source_month, zone_id)`; `zone_key = mês:ID`. |
| `gold.fct_trips` → `gold.dim_zones` (origem) | Muitos registros para uma zona; `pickup_zone_key = zone_key`. |
| `gold.fct_trips` → `gold.dim_zones` (destino) | Muitos registros para uma zona; `dropoff_zone_key = zone_key`. |
| Fato → atividade por embarque | Agrupar por mês de origem, zona e hora. |
| Fato → atividade mensal | Agrupar pelo mês real do embarque. |
| Fato → duração por rota | Agrupar por mês de origem e par de zonas. |

O [catálogo](CATALOG.md) apresenta todos os objetos previstos. A escolha de dimensão mensal evita vincular registros a um lookup substituído por outra execução. Isso não comprova que o lookup baixado hoje representa historicamente o mês da viagem. Registros semelhantes não recebem IDs artificiais nem são eliminados.

## Transformações e qualidade

Tipos normalizados: timestamps locais sem fuso, IDs inteiros, distância double e valores monetários decimal(18,2). Timestamps inválidos, ordem temporal invertida ou zonas desconhecidas geram rejeição. Distâncias e valores suspeitos geram alertas preservados. Duração é a diferença em segundos dividida por 60. Os limites numéricos são heurísticas de triagem, não detecção de fraude.

A fato enriquece origem e destino por LEFT JOIN. A validação das chaves da dimensão e dos relacionamentos detecta ausências e multiplicação indevida por duplicidade. As agregações reconciliam contagem e tarifa com a Silver. A soma de um grupo com todas as tarifas nulas é zero, mas `missing_fare_count` na agregação mensal permite identificar a ausência.

## Reexecução, falhas e publicação

Cada gravação Bronze/Silver usa `replaceWhere` para substituir apenas a partição do mês solicitado, inclusive quando a quarentena fica vazia. A Gold é reconstruída a partir de todos os meses aceitos disponíveis nos schemas selecionados. Use um prefixo dedicado para não incluir meses de outro estudo.

`ops.month_status` registra preparação, não publicação da Gold: RUNNING → INGESTED → RUNNING → SUCCESS, ou FAILED. A construção da Gold exige todos os meses rastreados em SUCCESS e todos os meses solicitados presentes. Também verifica a geração da preparação e as contagens de entrada/saída antes de gravar os modelos.

O Job completo só termina com sucesso após os controles da Gold e os comentários do catálogo. `ops.run_events` registra a etapa `pipeline`; sua coluna de mês referencia o primeiro mês da execução e `run_id` identifica a execução inteira. Em caso de falha, alguma tabela Gold pode já ter sido atualizada. Não há transação atômica entre tabelas: bloqueie consumo até a conclusão de uma nova execução bem-sucedida.

A configuração limita o Job a uma execução ativa. Essa configuração não bloqueia notebooks avulsos nem outros Jobs: não sobreponha escritores nos mesmos schemas. Notebooks separados existem para diagnóstico e execução manual sequencial. Depois do teste de reprocessamento, reconstrua a Gold para atualizar metadados e resultados.

## Limitações

Arquivos baixados novamente substituem a versão atual no Volume; não há arquivo histórico imutável por revisão da fonte. Mudança de schema deve ser revisada; colunas novas precisam entrar no catálogo. Duração usa relógio local e pode ser afetada pelo horário de verão. P1 e P3 incluem registros fora do mês do arquivo; P2 filtra os meses calendário do trimestre. A análise deve informar essa diferença.

O MVP não comprova disponibilidade de produção, demanda reprimida, rentabilidade ou causalidade. Não há números de negócio declarados antes da execução e inspeção das evidências.
