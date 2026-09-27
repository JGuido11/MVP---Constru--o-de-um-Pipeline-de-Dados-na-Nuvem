# Evidências para finalizar a entrega

Este roteiro segue o enunciado anexado da disciplina. A organização textual está pronta; a conformidade completa depende das evidências e resultados reais.

| Evidência | Onde obter | Onde inserir no README |
|---|---|---|
| Arquivos no Volume e data de coleta | Catalog Explorer / registro do download | Carga dos Dados |
| Execução concluída e identificador | Job do pipeline | Pipeline de Dados |
| Tabelas Bronze, Silver e Gold persistidas | Catalog Explorer | Pipeline de Dados |
| Tipos e comentários das colunas | `07_catalog.py` e Catalog Explorer | Modelagem e Catálogo |
| Completude, categorias, extremos e duplicidade | `06_quality.py` | Qualidade de Dados |
| Contagens e motivos de rejeição/alerta | `analysis.py` e `month_status` | Qualidade de Dados |
| Respostas P1, P2 e P3 | `analysis.py` | Análise de Dados |
| Teste de replay, se executado | `04_test_reprocessing.py` | Qualidade de Dados |

Guarde capturas reais em `docs/evidences/`. No README da raiz, insira cada imagem com a sintaxe `![Descrição](docs/evidences/nome.png)` somente depois que o arquivo existir. Preserve nome da tabela/consulta e números legíveis; oculte credenciais se aparecerem. Nenhuma imagem foi incluída como se fosse execução real.

Para cada resposta, escreva: pergunta → consulta → resultado numérico → screenshot → interpretação → limitação. Na qualidade, informe percentuais e denominadores; uma linha pode ter mais de um motivo. Na autoavaliação, relate apenas a experiência efetiva do autor.

Checklist final:

- [ ] Todos os sete tópicos exigidos aparecem explicitamente no README.
- [ ] Fontes, contexto e condições de uso descritos.
- [ ] Todos os campos efetivos revisados no catálogo, com tipos, domínios e linhagem.
- [ ] Evidências de persistência e resultados inseridas no próprio relatório.
- [ ] Resultados interpretados e objetivos não atingidos mantidos e discutidos.
- [ ] Autoavaliação individual preenchida.
- [ ] Repositório público e links locais funcionando.
- [ ] Link enviado no fórum oficial até 27/09/2026 às 23h59, conforme enunciado.

Dados brutos não precisam ser disponibilizados. Vídeos e áudios não são considerados pelo enunciado. Não declarar conclusão apenas porque os scripts existem.
