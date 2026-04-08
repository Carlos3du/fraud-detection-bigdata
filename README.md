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

## Fonte dos dados

Os dados utilizados neste projeto foram obtidos do [Kaggle](https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud), um repositório popular de conjuntos de dados para aprendizado de máquina e análise de dados. O conjunto de dados específico utilizado é o "Credit Card Fraud Detection", que contém transações de cartão de crédito, incluindo uma coluna de rótulo que indica se a transação é fraudulenta ou não. Este conjunto de dados é amplamente utilizado para treinar e avaliar modelos de detecção de fraudes em transações financeiras.

A primeira análise dos dados pode ser encontrada no notebook [Data Analysis](src/aed.ipynb), onde exploramos as características das transações, a distribuição dos dados e identificamos padrões relevantes para a detecção de fraudes.

## Ferramentas utilizadas

-=-=-=- In Progress -=-=-=-

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
  - [ ] Finalizado
  - [ ] Pendente
- Transformação:
  - [ ] Em progresso
  - [ ] Finalizado
  - [ ] Pendente
