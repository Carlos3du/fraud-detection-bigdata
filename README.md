# Fraud Detection Big Data

Projeto de Big Data para deteccao de transacoes fraudulentas em cartao de credito, combinando ingestao em tempo quase real, armazenamento em formato analitico e experimentos de aprendizado de maquina.

## Equipe

| Foto | Nome | GitHub | Email |
|---|---|---|---|
| ![Carlos](https://github.com/Carlos3du.png?size=40) | Carlos Eduardo | [Carlos3du](https://github.com/Carlos3du) | [cepc@cesar.school](mailto:cepc@cesar.school) |
| ![Cristina](https://github.com/Criismnaga.png?size=40) | Cristina Matsunaga | [Criismnaga](https://github.com/Criismnaga) | [cm2@cesar.school](mailto:cm2@cesar.school) |
| ![Francisco](https://github.com/fantonioluz.png?size=40) | Francisco Antonio | [fantonioluz](https://github.com/fantonioluz) | [fco@cesar.school](mailto:fco@cesar.school) |
| ![Gabriel](https://github.com/Gabriel-Chaves0.png?size=40) | Gabriel Chaves | [Gabriel-Chaves0](https://github.com/Gabriel-Chaves0) | [gco@cesar.school](mailto:gco@cesar.school) |
| ![Lucas](https://github.com/LucasGdBS.png?size=40) | Lucas Gabriel | [LucasGdBS](https://github.com/LucasGdBS) | [lgbs@cesar.school](mailto:lgbs@cesar.school) |
| ![Maria](https://github.com/FernandaFBMarques.png?size=40) | Maria Fernanda Marques | [FernandaFBMarques](https://github.com/FernandaFBMarques) | [mffbm@cesar.school](mailto:mffbm@cesar.school) |
| ![Thiago](https://github.com/tharaujo17.png?size=40) | Thiago Henrique | [tharaujo17](https://github.com/tharaujo17) | [thas@cesar.school](mailto:thas@cesar.school) |

## Introducao

O uso de cartoes de credito faz parte do cotidiano financeiro moderno e gera um volume elevado de transacoes digitais. Esse crescimento tambem amplia a superficie para fraudes, que podem causar perdas financeiras relevantes para consumidores, lojistas e instituicoes financeiras.

Nesse contexto, sistemas capazes de processar eventos continuamente e identificar transacoes suspeitas com baixa latencia sao essenciais. O desafio e ainda maior porque fraudes sao eventos raros: no dataset utilizado, apenas 492 das 284.807 transacoes sao fraudulentas, o que representa aproximadamente 0,172% dos registros.

## Motivacao

A deteccao de fraude e um problema adequado para Big Data porque exige:

- processamento de grandes volumes de eventos;
- ingestao continua de transacoes;
- armazenamento rastreavel e escalavel;
- transformacao progressiva dos dados;
- modelos capazes de lidar com classes altamente desbalanceadas;
- monitoramento de metricas tecnicas e indicadores de negocio.

Este projeto demonstra uma prova de conceito de uma arquitetura desse tipo, usando ferramentas acessiveis e executaveis localmente com Docker.

## Objetivo

Desenvolver uma prova de conceito para deteccao de transacoes fraudulentas em cartao de credito, utilizando o dataset [Credit Card Fraud Detection](https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud), disponibilizado pelo grupo Machine Learning Group da Universite Libre de Bruxelles.

O projeto combina duas frentes principais:

- uma pipeline de dados com Kafka, MinIO e arquivos Parquet;
- experimentos de machine learning com modelos de classificacao, incluindo XGBoost e uma abordagem com SMOTE para balanceamento das classes.

## Arquitetura

A pipeline implementada segue a ideia da arquitetura Medallion, com separacao progressiva entre ingestao, transformacao e consumo analitico.

![Diagrama da pipeline](documentacao/Diagrama1.png)

Fluxo principal:

1. O producer baixa o dataset, quando necessario, e publica as transacoes no topico Kafka `creditcard`.
2. O broker Kafka recebe os eventos e permite o consumo continuo das mensagens.
3. O consumer valida, transforma e enriquece os registros.
4. As transacoes transformadas sao agrupadas em janelas de 24 segundos.
5. Os dados processados sao gravados no MinIO em formato Parquet, no bucket `silver`.

Componentes:

| Camada | Tecnologia | Papel |
|---|---|---|
| Ingestao | Kafka, kafka-python | Simular o fluxo continuo de transacoes |
| Processamento | Python, Pandas, scikit-learn | Validar, transformar e enriquecer os registros |
| Armazenamento | MinIO, Parquet | Persistir dados transformados em formato colunar |
| Orquestracao | Docker Compose, Poe the Poet | Subir e controlar os servicos locais |
| Modelagem | XGBoost, scikit-learn, imbalanced-learn | Treinar e avaliar modelos de classificacao |

## Metodologia

A metodologia foi dividida em quatro etapas:

1. Analise exploratoria dos dados, com estudo da distribuicao das classes, comportamento das variaveis anonimizadas e impacto do desbalanceamento.
2. Construcao da pipeline de ingestao e armazenamento, simulando a chegada continua de transacoes via Kafka.
3. Transformacao dos dados no consumer, incluindo validacao de schema, tratamento numerico, categorizacao de valores, criacao de faixas de horario e persistencia em Parquet.
4. Treinamento e avaliacao de modelos de classificacao, priorizando metricas adequadas para problemas desbalanceados.

Notebooks:

- [Analise exploratoria](src/notebooks/aed.ipynb)
- [Modelo XGBoost](src/notebooks/model.ipynb)
- [Modelo com SMOTE](src/notebooks/model_smote.ipynb)

## Resultados

Foram realizados experimentos com XGBoost para classificar transacoes legitimas e fraudulentas. Devido ao forte desbalanceamento da base, a avaliacao considerou AUC-ROC, Precision, Recall e F1-Score.

O melhor modelo sem SMOTE utilizou profundidade maxima igual a 3, taxa de aprendizado de 0,039, `subsample` de 0,8 e `colsample_bytree` de 0,9. Essa configuracao alcancou AUC-ROC de 0,9836 no conjunto de validacao.

No conjunto de testes, a matriz de confusao foi:

| Classe real | Predito normal | Predito fraude |
|---|---:|---:|
| Normal | 56.859 | 3 |
| Fraude | 22 | 78 |

Metricas obtidas:

| Metrica | Valor |
|---|---:|
| AUC-ROC | 0,9836 |
| Precision | 96,30% |
| Recall | 78,00% |
| F1-Score | 86,19% |
| Acuracia | 99,96% |

Tambem foi avaliada uma abordagem com SMOTE. Apos otimizacao de hiperparametros por Randomized Search, o modelo alcancou AUC-ROC de 0,9821 e Recall de 85%. Essa versao aumentou a capacidade de identificar fraudes, mas apresentou desempenho geral semelhante ao modelo treinado diretamente sobre os dados originais.

## Conclusao

O projeto demonstrou a viabilidade de uma solucao local de Big Data para deteccao de fraudes em cartao de credito. A pipeline com Kafka, MinIO e Parquet permite simular a ingestao continua de transacoes e organizar os dados transformados de maneira rastreavel.

Do ponto de vista de modelagem, o XGBoost apresentou desempenho forte mesmo diante de um dataset altamente desbalanceado, com baixa quantidade de falsos positivos e boa capacidade de deteccao da classe fraudulenta. A abordagem com SMOTE trouxe ganho em Recall, reforcando a importancia de avaliar diferentes estrategias quando o custo de nao detectar uma fraude e alto.

Como proximos passos, a arquitetura pode evoluir para incluir uma camada Gold com predicoes em tempo real, dashboards de monitoramento, versionamento de modelos e metricas de negocio como valor exposto, valor potencialmente salvo e custo de falsos positivos.

## Setup

Este projeto usa [uv](https://docs.astral.sh/uv/) para gerenciar dependencias.

1. Clone o repositorio:

    ```bash
    git clone https://github.com/Carlos3du/fraud-detection-bigdata
    ```

2. Acesse o diretorio do projeto:

    ```bash
    cd fraud-detection-bigdata
    ```

3. Instale o `uv`, caso ainda nao tenha:

    ```bash
    pip install uv
    ```

4. Instale as dependencias:

    ```bash
    uv sync
    ```

5. Configure as variaveis de ambiente:

    ```bash
    cp .env.example .env
    ```

    Edite o arquivo `.env` com as credenciais do MinIO:

    ```env
    MINIO_ROOT_USER=seu_usuario
    MINIO_ROOT_PASSWORD=sua_senha
    ```

6. Suba os servicos:

    ```bash
    uv run poe up
    ```

7. Para acompanhar os logs:

    ```bash
    uv run poe logs
    ```

8. Para parar os servicos:

    ```bash
    uv run poe down
    ```

## Acessos locais

| Servico | URL |
|---|---|
| Kafdrop | <http://localhost:9000> |
| MinIO API | <http://localhost:9001> |
| MinIO Console | <http://localhost:9090> |

## Estrutura do projeto

```text
.
├── docker-compose.yml
├── pyproject.toml
├── src
│   ├── ingestion
│   │   ├── consumer
│   │   └── producer
│   ├── notebooks
│   └── scripts
├── documentacao
└── README.md
```
