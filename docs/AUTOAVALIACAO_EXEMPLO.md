# Exemplo de autoavaliação — MVP de Engenharia de Dados

> **Modelo para adaptação, não relato de execução.** Substitua os campos `{{...}}`, mantenha apenas afirmações comprovadas e revise o texto com sua experiência pessoal. Ao finalizar, copie a autoavaliação para a seção 7 do README. Não entregue os campos sem preencher.

## Texto-base para adaptar

O objetivo deste trabalho foi construir um pipeline de dados na nuvem para analisar a atividade de táxis amarelos em Nova York entre janeiro e março de 2026. O projeto foi organizado no Databricks, utilizando PySpark e SQL para transformar arquivos públicos em tabelas Bronze, Silver e Gold. A proposta incluiu preservar a origem dos registros, avaliar sua qualidade e produzir informações para responder às perguntas de negócio.

Considero que o objetivo foi **{{atingido / parcialmente atingido / não atingido}}**, porque **{{descrever o que a execução e as evidências demonstram}}**. Na execução de **{{data e identificador}}**, foram processados **{{quantidade}}** registros dos meses **{{meses efetivamente carregados}}**. Desse total, **{{quantidade}}** foram aceitos e **{{quantidade}}** foram rejeitados. As evidências de **{{identificar figuras}}** apresentam a execução e a persistência dos dados. **{{Se não houve execução completa, substituir este trecho pela explicação da etapa alcançada e do impedimento.}}**

Em relação à primeira pergunta, sobre zonas e horários com maior atividade observada, **{{apresentar o resultado principal de P1 ou explicar por que não foi possível obtê-lo}}**. Para a segunda pergunta, sobre a variação mensal de viagens e valores registrados, **{{apresentar a conclusão de P2 e seus limites}}**. Quanto à terceira pergunta, sobre duração mediana das rotas, **{{apresentar o resultado de P3 ou a limitação que impediu a resposta}}**. Esses resultados permitem **{{explicar a contribuição concreta para o problema de mobilidade}}**, considerando apenas a cobertura da base utilizada.

A principal dificuldade que encontrei foi **{{problema realmente enfrentado}}**. Sua consequência foi **{{efeito na execução ou na análise}}**. Para tratá-la, **{{ação efetivamente realizada}}**, e verifiquei o resultado por meio de **{{teste, consulta ou figura}}**. Outra dificuldade foi **{{incluir apenas se ocorreu}}**. Esse processo contribuiu para meu entendimento de **{{aprendizado pessoal ligado à dificuldade}}**.

Na avaliação de qualidade, identifiquei **{{problemas observados, com quantidades ou percentuais}}**. A política de separar rejeições e manter alertas permitiu **{{avaliar o efeito observado dessa decisão}}**. A ausência de um identificador confiável por viagem exige cautela com registros idênticos: preservar a multiplicidade da fonte evita descartar viagens potencialmente legítimas, mas limita a identificação de duplicidades indevidas. **{{Se a verificação não encontrou problemas em algum atributo, registrar a checagem e seu resultado, sem generalizar para toda a base.}}**

O trabalho apresenta limitações. O recorte cobre somente três meses e as viagens registradas não representam toda a demanda por transporte. Os valores monetários podem conter anomalias e não representam lucro. Os horários locais podem apresentar ambiguidades em transições de horário de verão. Além disso, o snapshot de zonas corresponde à coleta e não comprova a classificação histórica de cada mês. **{{Explicar quais dessas limitações afetaram efetivamente suas conclusões.}}**

Como aprendizado, destaco **{{descrever o que aprendeu sobre modelagem, qualidade, rastreabilidade ou execução}}**. Para evoluir o projeto, proponho ampliar o período de análise, preservar versões dos arquivos de origem e comparar a sensibilidade das conclusões às regras de qualidade. Essas melhorias são trabalhos futuros e não funcionalidades já implementadas.

## Como registrar atendimento das perguntas

Use esta tabela antes de escrever a conclusão. Não marque uma pergunta como respondida apenas porque existe uma consulta.

| Pergunta | Situação | Evidência | Conclusão ou impedimento |
|---|---|---|---|
| P1 — Zonas e horários | {{respondida / parcial / não respondida}} | {{figura e consulta}} | {{resultado ou motivo}} |
| P2 — Variação mensal | {{respondida / parcial / não respondida}} | {{figura e consulta}} | {{resultado ou motivo}} |
| P3 — Duração por rota | {{respondida / parcial / não respondida}} | {{figura e consulta}} | {{resultado ou motivo}} |

## Exemplo alternativo para conclusão parcial

O parágrafo abaixo é uma opção de redação, válida apenas se descrever o que ocorreu:

> Considero o objetivo parcialmente atingido. Foi possível concluir **{{etapas comprovadas}}**, enquanto **{{etapa pendente}}** não foi finalizada devido a **{{motivo real}}**. Por isso, mantenho a pergunta **{{identificador}}** como não respondida. A limitação não permite concluir **{{conclusão que os dados ainda não sustentam}}**. O próximo passo será **{{ação concreta}}**, seguido de nova validação e registro das evidências.

## Revisão antes da entrega

- Substituir todos os campos e remover instruções editoriais.
- Conferir se números, meses, figuras e status correspondem à execução apresentada.
- Relatar dificuldades e aprendizados próprios, sem adotar exemplos como fatos pessoais.
- Manter as perguntas que não foram respondidas e explicar os motivos.
- Separar realizações, limitações e melhorias futuras.
- Inserir o texto final na seção **Autoavaliação** do README.
