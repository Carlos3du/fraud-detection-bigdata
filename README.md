# Fraud Detection

Projeto de BigData para detecção de transações fraudulentas em cartão de crédito em tempo real

## Equipe

|Foto|Nome|GitHub|Email|
|---|---|---|---|
|![Carlos](https://github.com/Carlos3du.png?size=40)|Carlos Eduardo|[Carlos3du](https://github.com/Carlos3du)|[cepc@cesar.school](mailto:cepc@cesar.school)|
|![Cristina](https://github.com/Criismnaga.png?size=40)|Cristina Matsunaga|[Criismnaga](https://github.com/Criismnaga)|[cm2@cesar.school](mailto:cm2@cesar.school)|
|![Francisco](https://github.com/fantonioluz.png?size=40)|Francisco Antônio|[fantonioluz](https://github.com/fantonioluz)|[fco@cesar.school](mailto:fco@cesar.school)|
|![Gabriel](https://github.com/Gabriel-Chaves0.png?size=40)|Gabriel Chaves|[Gabriel-Chaves0](https://github.com/Gabriel-Chaves0)|[gco@cesar.school](mailto:gco@cesar.school)|
|![Lucas](https://github.com/LucasGdBS.png?size=40)|Lucas Gabriel|[LucasGdBS](https://github.com/LucasGdBS)|[lgbs@cesar.school](mailto:lgbs@cesar.school)|
|![Maria](https://github.com/FernandaFBMarques.png?size=40)|Maria Fernanda Marques|[FernandaFBMarques](https://github.com/FernandaFBMarques)|[mffbm@cesar.school](mailto:mffbm@cesar.school)|
|![Thiago](https://github.com/tharaujo17.png?size=40)|Thiago Henrique|[tharaujo17](https://github.com/tharaujo17)|[thas@cesar.school](mailto:thas@cesar.school)|

## Introdução

O uso de cartões de crédito tornou-se parte indissociável do cotidiano moderno, impulsionando um crescimento exponencial no volume de transações financeiras digitais. Esse cenário, embora reflita avanços significativos na economia digital e na inclusão financeira, também cria um ambiente propício para práticas fraudulentas e golpes. A cada ano, bilhões de dólares são perdidos globalmente em decorrência de fraudes em cartões de crédito, impactando tanto consumidores quanto instituições financeiras.Nesse contexto, a capacidade de identificar transações suspeitas de forma rápida e precisa torna-se um diferencial estratégico e uma necessidade operacional. Sistemas capazes de processar e analisar grandes volumes de dados em tempo real são essenciais para mitigar danos e proteger os envolvidos.

## Motivação (A FAZER)

## Objetivo do Projeto
Este projeto tem como objetivo o desenvolvimento de uma prova de conceito para a detecção de transações fraudulentas em tempo real, empregando técnicas de aprendizado de máquina e big data. Para isso, será utilizado o dataset [Credit Card Fraud Detection](https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud), disponibilizado pelo grupo MLG da Universidade Livre de Bruxelas (ULB) e embora seja de dimensão reduzida, toda a arquitetura é concebida sob a ótica de Big Data, demonstrando como soluções desse tipo se comportariam em ambientes de produção com volumes massivos de dados.

O dataset é composto por transações anonimizadas de cartões de crédito europeus e é amplamente adotado em estudos sobre o tema. O conjunto de dados reúne transações realizadas por portadores de cartões de crédito europeus em setembro de 2013, ao longo de dois dias. São 284.807 transações no total, das quais apenas 492 correspondem a fraudes — representando aproximadamente 0,172% dos registros, o que caracteriza um dataset altamente desbalanceado.

O pipeline é estruturado seguindo a arquitetura Medallion, com separação clara de responsabilidades entre camadas, garantindo rastreabilidade, reprodutibilidade e qualidade progressiva dos dados ao longo do fluxo.

A camada Gold representa a entrega de valor do projeto. Nela, cada transação recebe um score de probabilidade de fraude e uma classificação binária, gerados pelo modelo em tempo real. A partir dessas predições, são consolidados os principais indicadores de negócio: 
- valor total exposto a fraude,
- valor potencialmente salvo pela detecção correta e
- custo estimado dos falsos positivos. 

As transações são ainda segmentadas por nível de risco — baixo, médio e alto — e o desempenho do modelo é monitorado continuamente por meio de métricas como AUPRC, F1-score, Precision e Recall.


## Metodologia (A FAZER)

A primeira análise dos dados pode ser encontrada no notebook [Data Analysis](src/aed.ipynb), onde exploramos as características das transações, a distribuição dos dados e identificamos padrões relevantes para a detecção de fraudes.

### Ferramentas utilizadas

#### Ingestão de dados
- Apache Kafka
- kafka-python
- Kaggle
- Kaggle CLI

#### Processamento e transformação
- Python
- Pandas
- scikit-learn

#### Armazenamento
- MinIO
- Parquet

#### Orquestração e ambiente
- Docker
- Docker Compose

## Resultados

Foram realizados experimentos utilizando o algoritmo XGBoost para a classificação de transações fraudulentas e legítimas. Devido ao elevado desbalanceamento da base de dados, a principal métrica adotada para avaliação foi a Área Sob a Curva ROC (AUC-ROC), complementada pelas métricas Precision, Recall e F1-Score, que permitem uma análise mais adequada do desempenho sobre a classe minoritária.

Durante a etapa de treinamento, diferentes combinações de hiperparâmetros foram avaliadas com o objetivo de identificar a configuração de melhor desempenho. O modelo selecionado apresentou profundidade máxima igual a 3, taxa de aprendizado de 0,039, subsample de 0,8 e colsample_bytree de 0,9. Essa configuração alcançou AUC-ROC de 0,9836 no conjunto de validação, evidenciando elevada capacidade de distinguir transações fraudulentas de transações legítimas.

Após a seleção do modelo, foi realizada sua avaliação no conjunto de testes. Os resultados obtidos são apresentados na Tabela X, por meio da matriz de confusão, permitindo analisar a distribuição de acertos e erros entre as classes de transações legítimas e fraudulentas.


A avaliação no conjunto de testes produziu a matriz de confusão apresentada na Tabela X.

| Classe Real | Predito Normal | Predito Fraude |
|------------|---------------:|---------------:|
| Normal     | 56.859         | 3              |
| Fraude     | 22             | 78             |

Com base nesses resultados, foram obtidas as seguintes métricas:

| Métrica   | Valor   |
|-----------|---------:|
| AUC-ROC   | 0,9836   |
| Precision | 96,30%   |
| Recall    | 78,00%   |
| F1-Score  | 86,19%   |
| Acurácia  | 99,96%   |

Os resultados demonstram excelente desempenho na identificação de transações fraudulentas. O modelo foi capaz de detectar corretamente 78% das fraudes presentes no conjunto de testes, ao mesmo tempo em que manteve uma taxa extremamente baixa de falsos positivos, classificando incorretamente apenas três transações legítimas como fraudulentas.

Também foi avaliada uma abordagem utilizando a técnica SMOTE para balanceamento das classes. Após otimização dos hiperparâmetros por meio de Randomized Search, o modelo alcançou AUC-ROC de 0,9821 e Recall de 85%. Embora tenha apresentado ganho na capacidade de detectar fraudes, o desempenho geral foi semelhante ao modelo treinado diretamente sobre os dados originais, evidenciando a robustez do algoritmo XGBoost mesmo diante de um conjunto altamente desbalanceado.

Sob a perspectiva de negócio, os resultados indicam que a solução possui potencial para reduzir significativamente perdas financeiras associadas a fraudes em cartões de crédito, mantendo baixo impacto sobre transações legítimas. Além disso, a arquitetura proposta demonstrou a viabilidade de utilização de tecnologias de Big Data para processamento e classificação de eventos em tempo real, permitindo escalabilidade e monitoramento contínuo das predições geradas pelo modelo.

Além do desempenho preditivo, o projeto demonstrou a viabilidade da construção de um pipeline de detecção de fraudes em tempo real utilizando uma arquitetura baseada em Big Data. A utilização do padrão Medallion permitiu organizar o fluxo de dados em diferentes níveis de refinamento, garantindo rastreabilidade, qualidade dos dados e facilidade de monitoramento das predições geradas pelo modelo.

## Conclusão (A FAZER)

## Setup

Este projeto usa [uv](https://docs.astral.sh/uv/) para gerenciar as dependências. Para configurar o ambiente, siga os passos abaixo:

1. Clone o repositório:

    ```bash
    git clone https://github.com/Carlos3du/fraud-detection-bigdata
    ```

2. Navegue até o diretório do projeto:

    ```bash
    cd fraud-detection-bigdata
    ```

3. Instale o uv (caso não tenha) e as dependências do projeto:

    ```bash
    pip install uv
    uv sync
    ```

4. Configure as variáveis de ambiente copiando o arquivo de exemplo:

    ```bash
    cp .env.example .env
    ```

    Edite o `.env` definindo as credenciais do MinIO:

    ```env
    MINIO_ROOT_USER=seu_usuario
    MINIO_ROOT_PASSWORD=sua_senha
    ```

5. Para executar a pipeline, use o comando:

    ```bash
    uv run poe up
    uv run poe down # para parar os serviços
    ```
    
--- DAQUI PRA BAIXO POR CRISTINA DELETA TUDO ----
## Requisitos do projeto

- [ ] Diagrama do pipeline de dados atual (ingestão, armazenamento e transformação)
- [ ] Tecnologias já utilizadas e quais poderiam ser usadas para refinamento (tecnologias pagas) e justificativa da escolha
- [ ] Arquitetura parcial implementada (mesmo que em ambiente simulado).
- [ ] Equipe responsável e divisão de tarefas.

## CheckList progresso da Pipeline

- Ingestão:
  - [ ] Em progresso
  - [x] Finalizado
  - [ ] Pendente
- Armazenamento:
  - [ ] Em progresso
  - [x] Finalizado
  - [ ] Pendente
- Transformação:
  - [ ] Em progresso
  - [x] Finalizado
  - [ ] Pendente
