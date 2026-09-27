# Catálogo de Dados

Catálogo padrão: `workspace`; schemas: `mobility_bronze`, `mobility_silver`, `mobility_gold` e `mobility_ops`.

Os campos listados abaixo têm descrição, domínio e linhagem no dicionário seguinte. Campos herdados mantêm essas definições. Os tipos nativos da Bronze e as precisões calculadas devem ser transcritos a partir de `07_catalog.py` após executar: o código preserva o esquema da fonte. A capitalização dos campos é preservada da fonte (por exemplo, `Airport_fee` pode corresponder a `airport_fee` no dicionário). Colunas extras da fonte exigem revisão do catálogo. Não há chave única confiável por viagem.

## bronze.yellow_trips

Um registro original de viagem; esquema físico preservado do Parquet.

Colunas: `VendorID`, `tpep_pickup_datetime`, `tpep_dropoff_datetime`, `passenger_count`, `trip_distance`, `RatecodeID`, `store_and_fwd_flag`, `PULocationID`, `DOLocationID`, `payment_type`, `fare_amount`, `extra`, `mta_tax`, `tip_amount`, `tolls_amount`, `improvement_surcharge`, `total_amount`, `congestion_surcharge`, `airport_fee`, `cbd_congestion_fee`, `source_month`, `source_filename`, `ingested_at`, `ingestion_run_id`.

## bronze.taxi_zones

Uma linha original do lookup por mês de coleta; CSV como texto.

Colunas: `LocationID`, `Borough`, `Zone`, `service_zone`, `source_month`, `source_filename`, `ingested_at`, `ingestion_run_id`.

## silver.trips

Uma linha por registro aceito; multiplicidade da fonte preservada.

Colunas: `pickup_at`, `dropoff_at`, `pickup_zone_id`, `dropoff_zone_id`, `trip_distance_miles`, `fare_amount`, `total_amount`, `source_month`, `source_filename`, `ingested_at`, `ingestion_run_id`, `rejection_reasons`, `quality_warnings`, `duration_minutes`, `preparation_run_id`.

## silver.rejected_trips

Uma linha por registro rejeitado e seus motivos.

Colunas: `pickup_at`, `dropoff_at`, `pickup_zone_id`, `dropoff_zone_id`, `trip_distance_miles`, `fare_amount`, `total_amount`, `source_month`, `source_filename`, `ingested_at`, `ingestion_run_id`, `rejection_reasons`, `quality_warnings`, `duration_minutes`, `preparation_run_id`.

## silver.taxi_zones

Uma zona por mês de coleta.

Colunas: `zone_id`, `borough`, `zone_name`, `service_zone`, `source_month`.

## gold.dim_zones

Dimensão: uma zona por source_month; chave composta.

Colunas: `zone_key`, `source_month`, `zone_id`, `borough`, `zone_name`, `service_zone`.

## gold.stg_trips

View intermediária: uma linha por registro aceito.

Colunas: `pickup_at`, `dropoff_at`, `pickup_zone_id`, `dropoff_zone_id`, `trip_distance_miles`, `fare_amount`, `total_amount`, `source_month`, `source_filename`, `ingested_at`, `ingestion_run_id`, `rejection_reasons`, `quality_warnings`, `duration_minutes`, `preparation_run_id`, `pickup_zone_key`, `dropoff_zone_key`, `pickup_date`, `pickup_month`, `pickup_hour`, `has_quality_warning`.

## gold.fct_trips

Fato: uma linha por registro aceito com duas referências à mesma dimensão de zonas.

Colunas: `pickup_at`, `dropoff_at`, `pickup_zone_id`, `dropoff_zone_id`, `trip_distance_miles`, `fare_amount`, `total_amount`, `source_month`, `source_filename`, `ingested_at`, `ingestion_run_id`, `rejection_reasons`, `quality_warnings`, `duration_minutes`, `preparation_run_id`, `pickup_zone_key`, `dropoff_zone_key`, `pickup_date`, `pickup_month`, `pickup_hour`, `has_quality_warning`, `pickup_zone_name`, `pickup_borough`, `dropoff_zone_name`, `dropoff_borough`.

## gold.mart_pickup_activity

Uma linha por mês de origem, zona e hora de embarque.

Colunas: `source_month`, `pickup_zone_id`, `pickup_zone_name`, `pickup_hour`, `trip_count`, `recorded_fare_amount`, `unusual_trip_count`.

## gold.mart_monthly_activity

Uma linha por mês calendário de embarque.

Colunas: `pickup_month`, `trip_count`, `recorded_fare_amount`, `recorded_total_amount`, `missing_fare_count`, `unusual_trip_count`.

## gold.mart_route_duration

Uma linha por mês de origem e par origem/destino.

Colunas: `source_month`, `pickup_zone_id`, `dropoff_zone_id`, `pickup_zone_name`, `dropoff_zone_name`, `trip_count`, `average_duration_minutes`, `median_duration_minutes`, `unusual_trip_count`.

## ops.month_status

Uma linha de estado de preparação por mês; SUCCESS aqui não garante Gold publicada.

Colunas: `source_month`, `run_id`, `status`, `input_count`, `accepted_count`, `rejected_count`, `unusual_count`, `updated_at`.

## ops.run_events

Eventos de execução append-only; evento pipeline usa primeiro mês como referência para a execução completa.

Colunas: `source_month`, `run_id`, `service`, `status`, `event_time`, `error_type`.

## ops.connectivity_probe

Tabela de conectividade.

Colunas: `probe_id`.

## ops.reprocessing_evidence

Opcional; uma evidência por etapa do teste de reprocessamento.

Colunas: `test_run_id`, `source_month`, `stage`, `recorded_at`, `evidence_json`.

## Dicionário dos campos

| Campo | Tipo | Descrição | Domínio esperado | Linhagem |
|---|---|---|---|---|
| VendorID | numérico nativo | Código do provedor da viagem | 1, 2, 6, 7; conferir versão da fonte | Parquet TLC, sem transformação na Bronze |
| tpep_pickup_datetime | timestamp nativo | Início registrado da viagem | Data/hora válida; fora do mês é possível na fonte | Parquet TLC |
| tpep_dropoff_datetime | timestamp nativo | Fim registrado da viagem | Data/hora válida; comparar ao início | Parquet TLC |
| passenger_count | numérico nativo | Quantidade informada de passageiros | Inteiro não negativo esperado; nulos e extremos devem ser investigados | Parquet TLC; preservado na Bronze |
| trip_distance | numérico nativo | Distância em milhas | Maior que zero esperado; acima de 100 recebe alerta na Silver | Parquet TLC |
| RatecodeID | numérico nativo | Código tarifário | 1–6 ou 99; valores diferentes requerem investigação | Parquet TLC |
| store_and_fwd_flag | string | Indicador de transmissão posterior | Y ou N; nulos devem ser medidos | Parquet TLC |
| PULocationID | numérico nativo | Identificador da zona de início/fim, respectivamente | IDs presentes no lookup; 264/265 podem indicar localização inespecífica | Parquet TLC |
| DOLocationID | numérico nativo | Identificador da zona de início/fim, respectivamente | IDs presentes no lookup; 264/265 podem indicar localização inespecífica | Parquet TLC |
| payment_type | numérico nativo | Código de pagamento | 0–6 segundo o dicionário consultado | Parquet TLC |
| fare_amount | decimal(18,2) na Silver; nativo na Bronze | Tarifa registrada em USD | 0–500 como faixa de triagem; nulos, negativos e acima de 500 geram alerta | Parquet TLC; try_cast na Silver |
| total_amount | decimal(18,2) na Silver; nativo na Bronze | Total registrado em USD | Não negativo esperado; nulos e negativos geram alerta; sem teto imposto | Parquet TLC; try_cast na Silver |
| extra | numérico nativo | Adicionais tarifários em USD. | Não negativo esperado; ajustes negativos devem ser investigados, não apagados | Parquet TLC; somente Bronze, sem uso nas respostas |
| mta_tax | numérico nativo | Tributo MTA registrado em USD. | Não negativo esperado; ajustes negativos devem ser investigados, não apagados | Parquet TLC; somente Bronze, sem uso nas respostas |
| tip_amount | numérico nativo | Gorjeta registrada em USD; dinheiro não está incluído. | Não negativo esperado; ajustes negativos devem ser investigados, não apagados | Parquet TLC; somente Bronze, sem uso nas respostas |
| tolls_amount | numérico nativo | Pedágios registrados em USD. | Não negativo esperado; ajustes negativos devem ser investigados, não apagados | Parquet TLC; somente Bronze, sem uso nas respostas |
| improvement_surcharge | numérico nativo | Adicional de melhoria em USD. | Não negativo esperado; ajustes negativos devem ser investigados, não apagados | Parquet TLC; somente Bronze, sem uso nas respostas |
| congestion_surcharge | numérico nativo | Adicional estadual de congestionamento em USD. | Não negativo esperado; ajustes negativos devem ser investigados, não apagados | Parquet TLC; somente Bronze, sem uso nas respostas |
| airport_fee | numérico nativo | Taxa de embarque aeroportuário em USD. | Não negativo esperado; ajustes negativos devem ser investigados, não apagados | Parquet TLC; somente Bronze, sem uso nas respostas |
| cbd_congestion_fee | numérico nativo | Cobrança da zona de alívio de congestionamento em USD. | Não negativo esperado; ajustes negativos devem ser investigados, não apagados | Parquet TLC; somente Bronze, sem uso nas respostas |
| LocationID | string na Bronze | Identificador da zona | Inteiro conversível e único por snapshot mensal | CSV TLC; lido como texto |
| Borough | string | Região administrativa da zona | Categorias do lookup oficial; incluir casos desconhecidos | CSV TLC |
| Zone | string | Nome da zona | Nomes do lookup oficial | CSV TLC |
| service_zone | string | Classificação de serviço da zona | Categorias observadas no lookup, incluindo possíveis nulos | CSV TLC |
| source_month | string | Mês do arquivo/snapshot | YYYY-MM; escopo inicial 2026-01 a 2026-03 | Parâmetro de ingestão; não é o mês real da viagem |
| source_filename | string | Nome do arquivo ingerido | Nome não vazio de Parquet ou CSV | Caminho de entrada |
| ingested_at | timestamp | Instante da ingestão | Instante válido | current_timestamp() |
| ingestion_run_id | string | Identificador da execução da Bronze | Texto não vazio, até 256 caracteres | Parâmetro run_id |
| pickup_at | timestamp_ntz | Início/fim local da viagem, respectivamente | Não nulo nos aceitos; fim >= início | try_cast dos timestamps da Bronze |
| dropoff_at | timestamp_ntz | Início/fim local da viagem, respectivamente | Não nulo nos aceitos; fim >= início | try_cast dos timestamps da Bronze |
| pickup_zone_id | int | Identificador de zona | Membro do lookup; único por mês apenas na dimensão/lookup | Cast de PULocationID, DOLocationID ou LocationID |
| dropoff_zone_id | int | Identificador de zona | Membro do lookup; único por mês apenas na dimensão/lookup | Cast de PULocationID, DOLocationID ou LocationID |
| zone_id | int | Identificador de zona | Membro do lookup; único por mês apenas na dimensão/lookup | Cast de PULocationID, DOLocationID ou LocationID |
| trip_distance_miles | double | Distância normalizada em milhas | Faixa de triagem (0,100]; exceções mantidas com alerta | try_cast(trip_distance) |
| duration_minutes | decimal calculado pelo Spark | Duração em minutos | >= 0 nos aceitos; extremos devem ser analisados | timestampdiff(SECOND, pickup_at, dropoff_at) / 60.0 |
| rejection_reasons | array<string> | Regras impeditivas violadas | missing_or_invalid_timestamp; reversed_journey; invalid_pickup_zone; invalid_dropoff_zone; vazio nos aceitos | TripValidator.validate |
| quality_warnings | array<string> | Alertas não impeditivos | unusual_distance; unusual_fare; unusual_total; pickup_outside_source_month | TripValidator.validate; registros mantidos |
| preparation_run_id | string | Execução que produziu a Silver | Texto não vazio até 256 caracteres; igual ao run_id do status mensal | Parâmetro da preparação |
| borough | string | Região e nome da zona, respectivamente | Valores do lookup mensal | Borough e Zone renomeados |
| zone_name | string | Região e nome da zona, respectivamente | Valores do lookup mensal | Borough e Zone renomeados |
| zone_key | string | Chave composta da zona e mês | YYYY-MM:ID; zone_key única na dimensão | concat(source_month, dois-pontos, zone_id correspondente) |
| pickup_zone_key | string | Chave composta da zona e mês | YYYY-MM:ID; zone_key única na dimensão | concat(source_month, dois-pontos, zone_id correspondente) |
| dropoff_zone_key | string | Chave composta da zona e mês | YYYY-MM:ID; zone_key única na dimensão | concat(source_month, dois-pontos, zone_id correspondente) |
| pickup_date | date | Data local do início | Data válida | cast(pickup_at as date) |
| pickup_month | string | Mês calendário do início | YYYY-MM; pode diferir de source_month | date_format(pickup_at) |
| pickup_hour | int | Hora local do início | 0–23 | hour(pickup_at) |
| has_quality_warning | boolean | Presença de pelo menos um alerta | true ou false | size(quality_warnings) > 0 |
| pickup_zone_name | string | Nome/região de origem/destino conforme prefixo | Valores do lookup mensal; nomes podem ser desconhecidos | LEFT JOIN com dim_zones pelas chaves compostas |
| dropoff_zone_name | string | Nome/região de origem/destino conforme prefixo | Valores do lookup mensal; nomes podem ser desconhecidos | LEFT JOIN com dim_zones pelas chaves compostas |
| pickup_borough | string | Nome/região de origem/destino conforme prefixo | Valores do lookup mensal; nomes podem ser desconhecidos | LEFT JOIN com dim_zones pelas chaves compostas |
| dropoff_borough | string | Nome/região de origem/destino conforme prefixo | Valores do lookup mensal; nomes podem ser desconhecidos | LEFT JOIN com dim_zones pelas chaves compostas |
| trip_count | bigint | Número de registros aceitos no grupo | Inteiro >= 1 em grupos existentes | count(*) da fato |
| recorded_fare_amount | decimal(28,2) | Soma de tarifa/total registrado, respectivamente | Pode ser negativa devido aos alertas mantidos; soma totalmente nula vira zero | coalesce(sum(fare_amount ou total_amount),0) |
| recorded_total_amount | decimal(28,2) | Soma de tarifa/total registrado, respectivamente | Pode ser negativa devido aos alertas mantidos; soma totalmente nula vira zero | coalesce(sum(fare_amount ou total_amount),0) |
| unusual_trip_count | bigint | Quantidade com alerta / tarifa nula, respectivamente | 0 até trip_count | Soma de indicadores na fato |
| missing_fare_count | bigint | Quantidade com alerta / tarifa nula, respectivamente | 0 até trip_count | Soma de indicadores na fato |
| average_duration_minutes | decimal calculado pelo Spark | Duração média do grupo | >= 0 | avg(duration_minutes) |
| median_duration_minutes | mesmo tipo de duration_minutes | Mediana aproximada do grupo | >= 0 | percentile_approx(duration_minutes,0.5,10000) |
| run_id | string | Identificador da execução | Texto não vazio até 256 caracteres | Execução do pipeline/etapa |
| status | string | Estado da etapa ou execução | month_status: RUNNING, INGESTED, SUCCESS, FAILED; run_events: STARTED, SUCCESS, FAILED | Escrita pelo pipeline |
| input_count | bigint | Contagem de entrada, aceita, rejeitada ou aceita com alerta, conforme nome | Inteiro >= 0; nulo antes da etapa que calcula | Contagens de tabelas persistidas |
| accepted_count | bigint | Contagem de entrada, aceita, rejeitada ou aceita com alerta, conforme nome | Inteiro >= 0; nulo antes da etapa que calcula | Contagens de tabelas persistidas |
| rejected_count | bigint | Contagem de entrada, aceita, rejeitada ou aceita com alerta, conforme nome | Inteiro >= 0; nulo antes da etapa que calcula | Contagens de tabelas persistidas |
| unusual_count | bigint | Contagem de entrada, aceita, rejeitada ou aceita com alerta, conforme nome | Inteiro >= 0; nulo antes da etapa que calcula | Contagens de tabelas persistidas |
| updated_at | timestamp | Instante da atualização/evento | Timestamp válido | current_timestamp() |
| event_time | timestamp | Instante da atualização/evento | Timestamp válido | current_timestamp() |
| service | string | Etapa que emitiu o evento | ingestion, preparation ou pipeline | Classe da etapa |
| error_type | string | Classe da exceção | Texto; nulo em sucesso | Tipo da exceção capturada |
| probe_id | int | Marcador de conectividade | 1 | Bootstrap |
| test_run_id | string | Identificador do teste de replay | Texto não vazio | UUID do notebook opcional |
| stage | string | Etapa da evidência de replay | BEFORE, AFTER, PASS, FAIL | Notebook opcional de reprocessamento |
| recorded_at | timestamp | Instante da evidência | Timestamp válido | current_timestamp() |
| evidence_json | string | Snapshot de métricas ou resultado do teste | JSON válido; esquema varia com stage | Notebook de reprocessamento |

## Evidências pendentes

Execute `notebooks/07_catalog.py`, confira todas as colunas efetivas, copie tipos físicos/domínios observados e acrescente screenshots ao relatório. Domínio esperado e intervalo observado são informações diferentes.
