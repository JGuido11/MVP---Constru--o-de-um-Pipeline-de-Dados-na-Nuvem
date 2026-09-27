# MVP — Construção de um Pipeline de Dados na Nuvem

**Mobilidade urbana: viagens de táxi amarelo em Nova York — janeiro a março de 2026.**

Trabalho individual da pós-graduação em Data Science & Analytics da PUC-Rio.
Autor: João Paulo Guido. Entrega indicada no enunciado: **27/09/2026, às 23h59**.

Stack: **Databricks, PySpark, SQL, Delta Lake e Unity Catalog**. Um único Job executa o pipeline sequencialmente. A execução manual do mesmo notebook também é possível.

> Situação: implementação e documentação técnica disponíveis. A execução real desta versão no Databricks, os números das análises, os screenshots e a autoavaliação pessoal ainda precisam ser registrados. Este README não comprova uma execução que não foi apresentada.

## 1. Contexto de Negócios e Perguntas (Etapas 2 e 4.1)

O problema é organizar registros públicos de viagens para compreender a distribuição da atividade de táxis por local, horário e mês, além das durações típicas das rotas. Isso permite uma análise descritiva da mobilidade observada, condicionada à qualidade dos registros.

Objetivo: construir um pipeline reprodutível na nuvem que preserve a fonte, trate inconsistências, organize tabelas analíticas e sustente as perguntas abaixo.

1. **P1:** Quais zonas e horários concentram mais registros de viagens aceitas no período carregado?
2. **P2:** Como o volume de viagens e os valores registrados variam entre janeiro, fevereiro e março de 2026?
3. **P3:** Quais rotas/mês têm maior duração mediana, considerando grupos com pelo menos 100 registros aceitos?

As perguntas refletem as consultas existentes no projeto. Nenhuma deve ser removida por falta de resposta; registre a limitação na autoavaliação. Se houver objetivos originais adicionais definidos fora do repositório, acrescente-os e avalie seu atendimento.

A fonte é a NYC Taxi & Limousine Commission (TLC): três arquivos mensais Parquet e um CSV de zonas. Os registros incluem datas de início/fim, zonas, distância, passageiros, tarifas e códigos operacionais; o lookup contém `LocationID`, `Borough`, `Zone` e `service_zone`. Todos os atributos originais permanecem na Bronze. O [catálogo](docs/CATALOG.md) descreve os campos e transformações.

**Licença e termos de uso:** dados disponibilizados publicamente pela TLC para download. A página consultada não identifica uma licença padronizada como CC0 ou CC-BY; este trabalho não atribui uma licença não confirmada. São referenciados os [termos do NYC.gov](https://www.nyc.gov/main/terms-of-use) e o aviso da TLC de ausência de garantia de acurácia. A licença dos dados não deve ser confundida com a do código. Fontes, URLs dos arquivos e limitações estão em [DATA_SOURCES.md](docs/DATA_SOURCES.md).

## 2. Carga dos Dados (Etapa 4.2)

O procedimento padrão é baixar os arquivos oficiais e fazer upload no Volume gerenciado do Unity Catalog, mantendo uma pasta por mês. Alternativamente, `source_mode=download` realiza o download dentro do Databricks quando o acesso de rede permitir. Não é necessário publicar os arquivos de dados no GitHub.

O [guia de execução](docs/SETUP.md) explica criação do Volume, caminhos, parâmetros e execução. Código: [leitor da fonte](src/mobility/source.py), [bootstrap](notebooks/bootstrap.py) e [ingestão](notebooks/02_ingest_bronze.py).

**Evidência pendente:** screenshot do Volume com os arquivos e registro da data de download, tamanho dos arquivos e meses carregados.

## 3. Modelagem e Catálogo de Dados (Etapa 4.3)

A organização segue Bronze → Silver → Gold. A Gold adota uma fato de viagens ligada duas vezes à dimensão de zonas (origem e destino). A chave da dimensão combina mês do arquivo e ID da zona. As agregações atendem P1, P2 e P3. A granularidade da fato é um registro aceito da fonte; registros idênticos são preservados por falta de identificador único confiável.

- [Arquitetura, relacionamentos e decisões](docs/ARCHITECTURE.md).
- [Catálogo: tabelas, campos, tipos, domínios e linhagem](docs/CATALOG.md).
- [Notebook para conferir o esquema físico no Databricks](notebooks/07_catalog.py).

O pipeline aplica comentários nas tabelas e colunas do Unity Catalog a partir de `src/mobility/catalog.json`. A view intermediária tem documentação no catálogo Markdown. Novas colunas da fonte exigem atualização do dicionário.

**Evidência pendente:** screenshots do catálogo e transcrição dos tipos físicos efetivos, especialmente os tipos preservados da fonte e precisões calculadas pelo Spark.

## 4. Pipeline de Dados (Etapa 4.4)

O ponto de entrada é [01_run_pipeline.py](notebooks/01_run_pipeline.py). Ele cria a estrutura, processa Bronze e Silver para cada mês, verifica integridade, reconstrói a Gold com SQL nativo e documenta o catálogo. O Job configurado em [databricks.yml](databricks.yml) permite uma execução por vez. Também pode ser criado pela interface, com o mesmo notebook.

| Etapa | Entrada → saída | Transformação e propósito |
|---|---|---|
| Bronze | Arquivos → `yellow_trips`, `taxi_zones` | Preservação da fonte com metadados de origem e execução. |
| Silver | Bronze → `trips`, `rejected_trips`, `taxi_zones` | Tipagem, validação temporal/referencial, motivos de rejeição e alertas. |
| Gold | Silver → dimensão, fato e três agregados | Chaves compostas, calendário, enriquecimento de zonas e métricas. |
| Validação | Bronze/Silver/Gold/controle | Reconciliação de contagens, tarifas, relacionamentos e geração da preparação. |

Implementação: [processamento](src/mobility/jobs.py), [coordenação](src/mobility/pipeline.py), [Gold](src/mobility/analytics.py), [SQL dos modelos](sql/gold) e [SQL dos testes](sql/checks).

A substituição mensal evita duplicação por reexecução; não elimina duplicatas já presentes na fonte. A Gold é reconstruída integralmente. As escritas são atômicas por tabela, mas não constituem uma transação entre todas as tabelas. Consuma os resultados somente após sucesso integral do Job. Não execute notebooks avulsos simultaneamente com o Job.

**Evidência pendente:** screenshots do Job concluído, das tabelas persistidas e dos eventos de execução.

## 5. Qualidade de Dados (Etapa 4.5)

As regras implementadas estão em [validation.py](src/mobility/validation.py). Elas representam a política de tratamento, não resultados já observados.

| Situação | Tratamento | Impacto |
|---|---|---|
| Timestamp ausente/inválido; fim anterior ao início | Rejeitar e guardar motivo | Excluído das análises, preservado na Bronze e quarentena. |
| Zona ausente no lookup | Rejeitar e guardar motivo | Evita relacionamento inválido. |
| Distância nula/NaN, <= 0 ou > 100 milhas | Alertar e manter | A análise deve discutir extremos. |
| Tarifa nula, negativa ou > USD 500; total nulo/negativo | Alertar e manter | Os agregados incluem esses valores; não representam receita validada. |
| Início fora do mês do arquivo | Alertar e manter | P2 usa mês calendário; P1/P3 usam registros dos arquivos carregados. |
| Registros idênticos | Medir e preservar | Ausência de chave confiável impede concluir que sejam duplicatas indevidas. |
| Duração zero ou muito alta | Medir na análise exploratória | Não há exclusão automática; mediana não elimina todo viés. |

Execute [06_quality.py](notebooks/06_quality.py) para obter completude por atributo/mês, tipos, mínimo/máximo, quartis numéricos, categorias, duplicidade e comparação de tarifas com/sem alerta monetário. Valores distintos são aproximados; duplicidade é verificada por igualdade dos atributos originais dentro do mês. Acurácia aqui é plausibilidade e consistência interna, sem uma fonte externa para comprovar cada viagem.

**Resultados pendentes:** documentar quantidades e percentuais reais, problemas encontrados, atributos sem problemas e impacto dos tratamentos; incorporar screenshots ao relatório. As contagens por motivo de rejeição não são aditivas, porque uma linha pode ter vários motivos.

## 6. Análise de Dados (Etapa 4.5)

Execute [analysis.py](notebooks/analysis.py) somente após o sucesso integral do pipeline. SQL é suficiente para este MVP.

| Pergunta | Evidência produzida | Resultado e discussão |
|---|---|---|
| P1 | Top 20 combinações de zona e hora por registros aceitos | **Pendente de execução:** registrar líderes, quantidades, concentração e limites de cobertura. |
| P2 | Volumes e valores por mês calendário, com variação absoluta de viagens | **Pendente de execução:** registrar valores e direção da mudança; considerar dias por mês, completude e alertas monetários. |
| P3 | Top 20 rotas/mês por mediana, com mínimo de 100 viagens | **Pendente de execução:** registrar rotas, medianas e tamanhos dos grupos; discutir extremos e ausência de controle por distância. |

**Discussão geral pendente:** integrar as três respostas e relacioná-las ao problema. Não interpretar atividade observada como demanda total, valores registrados como lucro ou duração como prova de congestionamento. Horários locais sem fuso explícito não resolvem ambiguidades de horário de verão. Medianas são aproximadas.

Insira aqui os screenshots das respostas e a interpretação dos números reais. Use [EVIDENCES.md](docs/EVIDENCES.md) para organizar a coleta; não substitua evidências por imagens ilustrativas.

## 7. Autoavaliação

**Pendente de preenchimento pelo autor após executar a versão final.**

- Quais objetivos e perguntas foram atendidos? Quais ficaram sem resposta e por quê?
- Quais dificuldades ocorreram de fato na coleta, execução, modelagem e análise? Como foram resolvidas?
- O que foi aprendido sobre qualidade, rastreabilidade e reprocessamento?
- Limitações a avaliar: ausência de ID confiável, cobertura de três meses, valores anômalos mantidos e snapshots de zonas sem comprovação histórica.
- Trabalhos futuros possíveis: ampliar o período, preservar versões dos arquivos e avaliar a sensibilidade das conclusões às regras de qualidade.

Não é afirmado que todos os objetivos foram atingidos antes de haver resultados. A reflexão deve representar a experiência individual do autor.

## Verificação e entrega

Testes locais: `python -m pip install -r requirements-test.txt` e `python -m unittest discover -s tests -v`. O teste Spark local verifica transformações e controles com dados sintéticos; não comprova persistência Delta nem execução na nuvem. Verificação desta alteração: nove testes locais passaram com Spark 3.5.5, além de compilação Python, links Markdown e estrutura YAML. Para Delta no Databricks, utilize [acceptance.py](notebooks/acceptance.py) em schemas isolados; para replay real, [04_test_reprocessing.py](notebooks/04_test_reprocessing.py), seguido da reconstrução da Gold.

Antes de entregar: preencher resultados/autoavaliação, incorporar screenshots neste README, conferir catálogo, manter o repositório público e postar seu link no fórum da disciplina. Vídeos e áudios não substituem as evidências solicitadas. O enunciado permite até duas tentativas no fórum.
