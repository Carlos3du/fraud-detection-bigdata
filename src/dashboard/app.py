from io import BytesIO

import boto3
from botocore.exceptions import ClientError
import pandas as pd
import plotly.express as px
import streamlit as st
from decouple import config as env

MINIO_ENDPOINT = env("MINIO_ENDPOINT", default="http://minio:9000")
MINIO_ACCESS_KEY = env("MINIO_ACCESS_KEY", default="minioadmin")
MINIO_SECRET_KEY = env("MINIO_SECRET_KEY", default="minioadmin")
GOLD_BUCKET = env("GOLD_BUCKET", default="gold")
REFRESH_INTERVAL = int(env("REFRESH_INTERVAL", default="10"))
DASHBOARD_MAX_FILES = int(env("DASHBOARD_MAX_FILES", default="5000"))

st.set_page_config(
    page_title="Detecção de Fraudes",
    page_icon="🔍",
    layout="wide",
)


@st.cache_resource
def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


@st.cache_data(ttl=REFRESH_INTERVAL)
def load_gold_data() -> pd.DataFrame | None:
    try:
        s3 = get_s3_client()
        keys = []
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=GOLD_BUCKET, Prefix="predictions/"):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
            if len(keys) >= DASHBOARD_MAX_FILES:
                keys = keys[:DASHBOARD_MAX_FILES]
                break

        if not keys:
            return None

        frames = []
        for key in keys:
            obj = s3.get_object(Bucket=GOLD_BUCKET, Key=key)
            frames.append(pd.read_parquet(BytesIO(obj["Body"].read())))

        return pd.concat(frames, ignore_index=True)
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code == "NoSuchBucket":
            return None
        st.error(f"Erro ao carregar dados: {e}")
        return None
    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
        return None


def render_dashboard(df: pd.DataFrame) -> None:
    total = len(df)
    fraud_count = int(df["is_fraud_pred"].sum())
    fraud_rate = fraud_count / total * 100 if total > 0 else 0
    avg_prob = df["fraud_probability"].mean()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total de transações", f"{total:,}")
    col2.metric("Fraudes detectadas", f"{fraud_count:,}")
    col3.metric("Taxa de fraude", f"{fraud_rate:.2f}%")
    col4.metric("Probabilidade média", f"{avg_prob:.3f}")

    st.divider()

    col_left, col_right = st.columns(2)

    with col_left:
        st.subheader("Distribuição de risco")
        risk_counts = df["risk_level"].value_counts().reset_index()
        risk_counts.columns = ["Nível", "Quantidade"]
        color_map = {"alto": "#ef4444", "medio": "#f97316", "baixo": "#22c55e"}
        fig_pie = px.pie(
            risk_counts,
            names="Nível",
            values="Quantidade",
            color="Nível",
            color_discrete_map=color_map,
        )
        st.plotly_chart(fig_pie, use_container_width=True)

    with col_right:
        st.subheader("Probabilidade de fraude")
        fig_hist = px.histogram(
            df,
            x="fraud_probability",
            nbins=40,
            color_discrete_sequence=["#6366f1"],
            labels={"fraud_probability": "Probabilidade", "count": "Transações"},
        )
        fig_hist.add_vline(
            x=0.5,
            line_dash="dash",
            line_color="red",
            annotation_text="Threshold 0.5",
        )
        st.plotly_chart(fig_hist, use_container_width=True)

    st.subheader("Fraudes por categoria de valor")
    fraud_by_category = (
        df[df["is_fraud_pred"] == 1]["transaction_amount_category"]
        .value_counts()
        .reset_index()
    )
    fraud_by_category.columns = ["Categoria", "Fraudes"]
    order = ["baixo", "medio", "alto", "muito_alto"]
    fraud_by_category["Categoria"] = pd.Categorical(
        fraud_by_category["Categoria"], categories=order, ordered=True
    )
    fraud_by_category = fraud_by_category.sort_values("Categoria")
    fig_bar = px.bar(
        fraud_by_category,
        x="Categoria",
        y="Fraudes",
        color_discrete_sequence=["#ef4444"],
    )
    st.plotly_chart(fig_bar, use_container_width=True)

    st.subheader("Últimas transações classificadas")
    display_cols = [
        "transaction_time",
        "transaction_amount",
        "transaction_amount_category",
        "fraud_probability",
        "risk_level",
        "is_fraud_pred",
        "is_fraud",
    ]
    recent = df[display_cols].tail(50).sort_values("transaction_time", ascending=False)
    recent = recent.rename(
        columns={
            "transaction_time": "Tempo",
            "transaction_amount": "Valor (R$)",
            "transaction_amount_category": "Categoria",
            "fraud_probability": "Prob. Fraude",
            "risk_level": "Risco",
            "is_fraud_pred": "Pred. Fraude",
            "is_fraud": "Label Real",
        }
    )

    def highlight_fraud(row):
        if row["Pred. Fraude"] == 1:
            return ["background-color: #fef2f2"] * len(row)
        return [""] * len(row)

    st.dataframe(
        recent.style.apply(highlight_fraud, axis=1),
        use_container_width=True,
        hide_index=True,
    )


def main() -> None:
    st.title("🔍 Detecção de Fraudes em Tempo Real")
    st.caption(f"Dados em cache por {REFRESH_INTERVAL}s")

    with st.spinner(f"Carregando até {DASHBOARD_MAX_FILES:,} transações..."):
        df = load_gold_data()
    if df is None or df.empty:
        st.info("Aguardando dados da camada gold... Verifique se a pipeline está rodando.")
    else:
        render_dashboard(df)

    st.caption(f"Última atualização: {pd.Timestamp.now().strftime('%H:%M:%S')}")


if __name__ == "__main__":
    main()
