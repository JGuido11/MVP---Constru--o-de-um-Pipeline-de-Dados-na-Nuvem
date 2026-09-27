# Fontes, coleta e condições de uso

## Origem e recorte

Fonte primária: [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), consultada em 27/09/2026. Recorte: Yellow Taxi, janeiro a março de 2026, Nova York. A TLC distribui registros fornecidos pelos operadores tecnológicos e alerta que não garante sua acurácia. Os arquivos mensais estão em Parquet; o lookup de zonas é CSV.

| Arquivo | URL oficial |
|---|---|
| Janeiro | https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-01.parquet |
| Fevereiro | https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-02.parquet |
| Março | https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-03.parquet |
| Zonas | https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv |

Referência de campos: [dicionário oficial Yellow Taxi](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf). O arquivo efetivamente recebido determina os tipos físicos e pode apresentar campos adicionais. O catálogo do projeto deve acompanhar qualquer evolução.

## Coleta e rastreabilidade

Download manual dos quatro arquivos e upload em Volume, conforme [SETUP.md](SETUP.md), ou download opcional pelo `TripSourceReader` dentro do Databricks. Registre data da coleta, tamanho dos arquivos e URLs. O download programático verifica arquivo não vazio e marcadores Parquet; isso não substitui checksum nem leitura integral de validação. A ingestão verifica campos obrigatórios e existência de registros.

Cada mês recebe um snapshot do lookup. A Bronze acrescenta mês de origem, arquivo, instante de ingestão e identificador de execução. Os arquivos e a Bronze são preservados para investigar rejeições, mas rebaixar uma fonte modificada substitui a versão atual; não existe histórico imutável por versão do publicador.

## Licença e termos

A página da TLC disponibiliza os arquivos publicamente, mas não foi identificada nela uma licença padronizada explícita. Não declarar CC0, CC-BY ou domínio público sem confirmação. Referência de uso do site: [Terms of Use do NYC.gov](https://www.nyc.gov/main/terms-of-use), consultada em 27/09/2026; contém regras de uso, direitos de propriedade e ausência de garantias. A disponibilidade para download não deve ser descrita como cessão irrestrita de direitos.

Este trabalho referencia a origem e utiliza os arquivos para análise acadêmica. O repositório publica código e documentação, sem redistribuir a base. Não há alegação de endosso da TLC. A ausência de uma licença padronizada confirmada é registrada como limitação documental; eventuais condições específicas adicionais da fonte devem ser verificadas antes de reutilização fora deste escopo.

## Evidência a completar

Ainda não foram registrados nesta versão a data efetiva de download, os tamanhos, as contagens e screenshots da carga. Preencha-os com a execução real. Não use a data de consulta da documentação como se fosse data comprovada de ingestão.
