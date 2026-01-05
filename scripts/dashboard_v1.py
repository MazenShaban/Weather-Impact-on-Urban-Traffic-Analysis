import streamlit as st
import pandas as pd
import s3fs
from PIL import Image
import io

# --- Configuration ---
st.set_page_config(page_title="Big Data Traffic Analytics", layout="wide")

# MinIO Connection
MINIO_OPTS = {
    "key": "admin",
    "secret": "password123",
    "client_kwargs": {"endpoint_url": "http://localhost:9000"}
}

# --- Helper Functions ---
@st.cache_data
def load_parquet(path):
    try:
        return pd.read_parquet(f"s3://{path}", storage_options=MINIO_OPTS)
    except Exception as e:
        st.error(f"Error loading {path}: {e}")
        return pd.DataFrame()

@st.cache_data
def load_csv(path):
    try:
        return pd.read_csv(f"s3://{path}", storage_options=MINIO_OPTS)
    except Exception as e:
        st.error(f"Error loading {path}: {e}")
        return pd.DataFrame()

def load_image(path):
    fs = s3fs.S3FileSystem(**MINIO_OPTS)
    try:
        with fs.open(path, 'rb') as f:
            image_data = f.read()
            return Image.open(io.BytesIO(image_data))
    except Exception as e:
        return None

# --- Dashboard Layout ---

st.title("🚗💨 Big Data Project: Urban Traffic Risk Analysis")
st.markdown("### A Predictive Data Lake Pipeline using MinIO, HDFS, and Monte Carlo Simulations")

# --- Tabs ---
tab1, tab2, tab3 = st.tabs(["📊 Data Overview", "🎲 Monte Carlo Risk", "📉 Factor Analysis"])

# --- TAB 1: GOLD DATA ---
with tab1:
    st.header("The Golden Dataset")
    st.markdown("This data is the result of merging **Weather** and **Traffic** data sources.")
    
    # UPDATED PATH: merged_data subfolder
    df_merged = load_parquet("gold/merged_data/merged_data.parquet")
    
    if not df_merged.empty:
        city_filter = st.selectbox("Filter by City:", options=["All"] + list(df_merged['city'].unique()))
        
        if city_filter != "All":
            display_df = df_merged[df_merged['city'] == city_filter]
        else:
            display_df = df_merged
            
        st.dataframe(display_df.head(1000), use_container_width=True)
        st.caption(f"Showing top 1,000 rows. Total Data Points: {len(display_df)}")
        
        c1, c2, c3 = st.columns(3)
        c1.metric("Total Records", len(df_merged))
        c2.metric("Avg Traffic Volume", f"{int(df_merged['vehicle_count'].mean())} cars/hr")
        c3.metric("Avg Temp", f"{df_merged['temperature_c'].mean():.1f} °C")

# --- TAB 2: MONTE CARLO ---
with tab2:
    st.header("Monte Carlo Simulation Results")
    st.markdown("We simulated **5,000 future scenarios** for each weather condition.")
    
    st.subheader("📋 Risk Summary Table")
    # UPDATED PATH: monte_carlo subfolder
    df_summary = load_csv("gold/monte_carlo/simulation_summary.csv")
    if not df_summary.empty:
        st.table(df_summary)
    
    st.subheader("📈 Probability Distribution")
    # UPDATED PATH: monte_carlo subfolder
    img_dist = load_image("gold/monte_carlo/congestion_dist_plot.png")
    if img_dist:
        st.image(img_dist, caption="Kernel Density Estimate of Traffic Risk", use_container_width=True)

# --- TAB 3: FACTOR ANALYSIS (UPDATED) ---
with tab3:
    st.header("Factor Analysis & Dimensionality Reduction")
    st.markdown("Using **Kaiser Method** to determine factors and analyzing rotations.")

    # Section A: Kaiser Criterion & Scree Plot
    st.subheader("1. Determining Number of Factors (Kaiser Criterion)")
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.markdown("**Eigenvalues Table**")
        # UPDATED PATH: factor_analysis subfolder
        df_eigen = load_csv("gold/factor_analysis/fa_eigenvalues.csv")
        if not df_eigen.empty:
            # Highlight eigenvalues > 1 (Green)
            st.dataframe(
                df_eigen.style.applymap(
                    lambda x: 'background-color: #d4edda' if isinstance(x, (int, float)) and x > 1 else '', 
                    subset=['Eigenvalue']
                ),
                use_container_width=True
            )
    
    with col2:
        st.markdown("**Scree Plot**")
        # UPDATED PATH: factor_analysis subfolder
        img_scree = load_image("gold/factor_analysis/fa_scree_plot.png")
        if img_scree:
            st.image(img_scree, use_container_width=True)

    st.divider()

    # Section B: Rotation Results
    st.subheader("2. Factor Results by Rotation")
    
    # Dropdown to choose which file to load
    rotation = st.selectbox("Select Rotation Method:", ["varimax", "promax", "quartimax"])
    
    c1, c2 = st.columns(2)
    
    with c1:
        st.markdown(f"**a) Factor Loadings ({rotation.title()})**")
        # UPDATED PATH: factor_analysis subfolder
        df_loadings = load_csv(f"gold/factor_analysis/fa_loadings_{rotation}.csv")
        if not df_loadings.empty:
            st.dataframe(df_loadings, height=300, use_container_width=True)
            
    with c2:
        st.markdown(f"**b) Communalities ({rotation.title()})**")
        # UPDATED PATH: factor_analysis subfolder
        df_comm = load_csv(f"gold/factor_analysis/fa_communalities_{rotation}.csv")
        if not df_comm.empty:
            st.dataframe(df_comm, height=300, use_container_width=True)

    st.markdown(f"**c) Total Variance Explained ({rotation.title()})**")
    # UPDATED PATH: factor_analysis subfolder
    df_var = load_csv(f"gold/factor_analysis/fa_variance_{rotation}.csv")
    if not df_var.empty:
        st.table(df_var)

# Footer
st.markdown("---")
st.markdown("✅ **Pipeline Status:** End-to-End Execution Complete.")