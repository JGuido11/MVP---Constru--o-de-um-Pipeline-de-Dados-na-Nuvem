# Guia de screenshots — exemplos para o relatório

> **Roteiro de captura e redação.** Este documento não contém screenshots reais nem comprova execução. Os nomes de arquivos abaixo são sugestões. Substitua `{{...}}` com informações observadas e incorpore as capturas ao README após executá-las no Databricks.

## Como preparar as capturas

1. Execute a versão final do pipeline e anote a branch/commit, `run_id`, catálogo, prefixo e meses carregados.
2. Use o mesmo ambiente nos notebooks de análise, qualidade e catálogo.
3. Capture telas legíveis com nome da tabela ou consulta, colunas e valores relevantes. Se precisar, divida uma tabela larga em várias imagens identificadas.
4. Salve PNGs em `docs/evidences/`, com os nomes sugeridos abaixo ou equivalentes.
5. Insira cada imagem na seção correspondente do README, acompanhada de legenda e interpretação. Uma pasta de imagens sem explicação não substitui o relatório.

Evite exibir tokens ou credenciais. Não use telas sintéticas como evidência de dados reais. Testes com fixtures devem ser identificados como testes.

## Plano de capturas

A numeração é uma sugestão de organização, não uma quantidade obrigatória definida pela disciplina. Uma evidência pode precisar de mais de uma imagem.

| Figura / arquivo sugerido | Onde capturar | O que precisa aparecer | Seção do README |
|---|---|---|---|
| 01 — `01-volume.png` | Volume `landing` no Catalog Explorer | Caminho, pastas mensais e arquivos; abrir pastas em capturas adicionais se necessário. | Carga dos Dados |
| 02 — `02-job.png` | Execução do Job `mobility-pipeline` | Identificador, tarefa, estado final e data/horário. | Pipeline de Dados |
| 03 — `03-tabelas.png` | Schemas Bronze, Silver e Gold | Nomes dos objetos criados; complementar com detalhes de formato e amostras. | Pipeline de Dados |
| 04 — `04-catalogo.png` | Catalog Explorer e `07_catalog.py` | Tabela, colunas, tipos físicos, comentários, domínios e linhagem documentada. | Modelagem e Catálogo |
| 05 — `05-contagens.png` | `ops.month_status` | Mês, entrada, aceitos, rejeitados, alertas e estado. | Qualidade de Dados |
| 06 — `06-perfil.png` | `06_quality.py` | Completude, mínimos/máximos e quartis por atributo e mês; usar várias capturas para cobrir os campos. | Qualidade de Dados |
| 07 — `07-duplicidade.png` | `06_quality.py` | Grupos idênticos e linhas excedentes; resultado vazio também deve ser explicado. | Qualidade de Dados |
| 08 — `08-rejeicoes-alertas.png` | Últimas células de `analysis.py` | Motivos de rejeição/alerta e contagens por mês. | Qualidade de Dados |
| 09 — `09-sensibilidade.png` | Última célula de `06_quality.py` | Tarifas com/sem alerta monetário e distribuição/extremos de duração. | Qualidade de Dados |
| 10 — `10-p1.png` | Consulta P1 em `analysis.py` | Zona, hora e quantidade de registros no ranking. | Análise de Dados |
| 11 — `11-p2.png` | Consulta P2 em `analysis.py` | Três meses, volumes, valores registrados e variação absoluta. | Análise de Dados |
| 12 — `12-p3.png` | Consulta P3 em `analysis.py` | Mês, origem/destino, quantidade, média e mediana da duração. | Análise de Dados |
| 13 — `13-reprocessamento.png` (opcional) | `04_test_reprocessing.py` | Identificador do teste, comparações e resultado PASS/FAIL. | Qualidade de Dados |

## Exemplo 1 — Carga dos dados

**Legenda para adaptar:** Figura 1 — Arquivos de origem armazenados no Volume `{{caminho}}`, referentes a `{{meses}}`. Captura realizada em `{{data, horário e fuso}}`.

**Texto para adaptar:** Os arquivos foram obtidos nas URLs oficiais documentadas e enviados para o Volume gerenciado. A Figura 1 apresenta **{{arquivos e pastas visíveis}}**. A data efetiva de download foi **{{data registrada}}**. A presença dos arquivos comprova seu armazenamento; o processamento é demonstrado separadamente pelas evidências do Job e das tabelas.

## Exemplo 2 — Execução e persistência

**Legenda para adaptar:** Figura 2 — Execução `{{run_id}}` do Job `{{nome}}`, finalizada com estado `{{estado}}` em `{{data}}`.

**Texto para adaptar:** A execução processou **{{meses}}** e apresentou o estado **{{estado observado}}**. As Figuras **{{números}}** mostram os objetos persistidos em **{{catálogo e schemas}}**. **{{Se houve falha, explicar a etapa interrompida e não afirmar conclusão da Gold.}}**

A tabela `month_status` indica o estado da preparação. `SUCCESS` nessa tabela, isoladamente, não comprova sucesso da Gold; mostre também a conclusão do pipeline completo.

Para uma tabela Delta, você pode executar a consulta abaixo, ajustando o catálogo/prefixo, e capturar `format` e as demais informações relevantes. Ela não se aplica à view `stg_trips`:

```sql
DESCRIBE DETAIL workspace.mobility_gold.fct_trips;
```

## Exemplo 3 — Catálogo

**Legenda para adaptar:** Figura 4 — Estrutura e documentação da tabela `{{nome completo}}`, com tipos físicos e descrição dos campos.

**Texto para adaptar:** A tabela tem granularidade **{{uma linha representa o quê}}**. O campo **{{nome}}** possui tipo **{{tipo observado}}**, representa **{{significado}}**, admite **{{domínio esperado}}** e é derivado de **{{origem e transformação}}**. Os intervalos observados no perfil foram **{{mínimo/máximo ou categorias}}**, permitindo **{{avaliar consistência}}**.

O notebook `07_catalog.py` exibe a linhagem documentada no projeto. Não descreva essa saída como uma captura da linhagem automática do Unity Catalog se essa visualização não tiver sido obtida. Mantenha o catálogo textual completo; uma imagem de poucas colunas não o substitui.

## Exemplo 4 — Qualidade e reconciliação

**Legenda para adaptar:** Figura 5 — Reconciliação dos registros de entrada, aceitos e rejeitados por mês de origem.

**Texto para adaptar:** Em **{{mês}}**, foram identificados **{{N}}** registros de entrada, **{{A}}** aceitos e **{{R}}** rejeitados. A reconciliação **{{N}} = {{A}} + {{R}}** apresentou resultado **{{válido/inválido}}**. O percentual de rejeição foi **{{100 × R / N}}%**, considerando a entrada como denominador. Entre os aceitos, **{{W}}** possuíam pelo menos um alerta, correspondendo a **{{100 × W / A}}%**. Esses indicadores foram obtidos das tabelas persistidas.

**Texto para os motivos:** Os principais motivos observados foram **{{motivos e contagens}}**. Uma linha pode apresentar vários motivos; portanto, a soma dessas contagens pode superar a quantidade de linhas rejeitadas. Os alertas não implicam exclusão nem comprovação de fraude.

**Texto para duplicidade:** A verificação encontrou **{{quantidade}}** grupos de registros idênticos dentro de cada mês, com **{{quantidade}}** linhas excedentes. A comparação utilizou os atributos originais e excluiu metadados de execução. Os registros foram preservados porque a igualdade dos atributos não comprova duplicidade indevida na ausência de ID confiável.

Se a consulta de duplicidade estiver vazia, explique que não foram encontrados grupos idênticos no conjunto verificado; não conclua que toda a base está livre de qualquer problema de qualidade. Não use zero sem ter executado a verificação.

## Exemplo 5 — Respostas às perguntas de negócio

### P1 — Zonas e horários

**Legenda:** Figura 10 — Ranking de combinações de zona e hora por quantidade de registros aceitos nos arquivos carregados.

**Texto para adaptar:** A combinação de **{{zona}}** e **{{hora}}** apresentou **{{quantidade}}** registros, ocupando a posição **{{posição}}**. O resultado indica **{{interpretação descritiva}}** no recorte observado. O ranking não mede demanda reprimida. Ele utiliza os registros aceitos dos arquivos carregados, inclusive eventuais embarques fora do mês de origem que foram mantidos com alerta.

### P2 — Comparação mensal

**Legenda:** Figura 11 — Volume de registros aceitos e valores registrados por mês calendário de embarque, entre janeiro e março de 2026.

**Texto para adaptar:** O volume passou de **{{N1}}** em **{{mês 1}}** para **{{N2}}** em **{{mês 2}}**, uma diferença de **{{N2 − N1}}** registros. **{{Se calcular percentual, usar 100 × (N2 − N1) / N1 quando N1 > 0 e explicitar que se trata de cálculo adicional.}}** Os valores registrados foram **{{valores}}**. A comparação deve considerar a quantidade de dias dos meses e as anomalias monetárias mantidas. Não é possível inferir lucro a partir dessas somas.

A variação do primeiro mês aparece nula porque a consulta não inclui um mês anterior dentro do recorte. Não interpretar esse nulo como variação zero. Não inventar um mês ausente para completar a tabela.

### P3 — Duração por rota

**Legenda:** Figura 12 — Rotas/mês com maior duração mediana aproximada, restritas a grupos com pelo menos 100 registros aceitos.

**Texto para adaptar:** A rota de **{{origem}}** para **{{destino}}**, em **{{mês de origem}}**, apresentou mediana de **{{minutos}}** minutos e **{{quantidade}}** registros. O resultado descreve a duração típica desse grupo, sem controlar distância, horário ou condições de trânsito. Portanto, não comprova que essa seja a rota mais congestionada. **{{Relacionar aos extremos observados e às limitações de horário local.}}**

Se não houver grupos com 100 registros, apresente o resultado vazio e explique que P3 não pôde ser respondida com esse critério e recorte.

## Exemplo de inserção no README

O bloco abaixo usa caminho relativo à raiz do repositório. Copie-o para o README **somente após criar o arquivo da captura real**. Enquanto estiver aqui, é apenas um exemplo de código, não uma imagem ausente no relatório.

```markdown
### Evidência da pergunta P1

![Resultado da consulta P1: zonas, horários e registros aceitos](docs/evidences/10-p1.png)

*Figura 10 — Ranking de zonas e horários. Fonte: execução própria no Databricks,
com dados da NYC TLC. Captura em {{data}}; execução {{run_id}}.*

A combinação de {{zona}} e {{hora}} apresentou {{quantidade}} registros.
O resultado indica {{interpretação}}, limitado ao recorte e às regras de qualidade descritos.
```

Para inserir a imagem em um arquivo dentro de `docs/`, o caminho relativo é `evidences/10-p1.png`.

## Registro de rastreabilidade para preencher

| Item | Informação real |
|---|---|
| Commit/branch executado | {{identificador}} |
| Execução do pipeline | {{run_id e data}} |
| Catálogo e prefixo | {{valores}} |
| Meses de origem carregados | {{lista}} |
| Data efetiva de download | {{data registrada}} |
| Data e fuso das capturas | {{valores}} |
| Resultado do pipeline | {{estado}} |
| Limitações da execução | {{descrição ou ausência de impedimentos observados}} |

Confira se todas as imagens e números correspondem à mesma execução ou identifique explicitamente execuções diferentes. O [roteiro de entrega](EVIDENCES.md) e o [modelo de autoavaliação](AUTOAVALIACAO_EXEMPLO.md) complementam este guia.
