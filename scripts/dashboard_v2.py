import streamlit as st
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import FactorAnalysis
import s3fs
import io

# Optional: Try importing factor_analyzer for KMO/Bartlett
try:
    from factor_analyzer.factor_analyzer import calculate_kmo, calculate_bartlett_sphericity
    FACTOR_ANALYZER_AVAILABLE = True
except Exception:
    FACTOR_ANALYZER_AVAILABLE = False

# ===========================
# Configuration & MinIO Connection
# ===========================
st.set_page_config(
    page_title="Phase 7 — Traffic & Weather Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# MinIO Connection Details
MINIO_OPTS = {
    "key": "admin",
    "secret": "password123",
    "client_kwargs": {"endpoint_url": "http://localhost:9000"}
}

st.title("🚦 Phase 7 — Traffic & Weather Dashboard")
st.markdown("Interactive dashboard connecting to **MinIO Gold Layer**. Performs real-time filtering and analysis.")
st.markdown("---")

# ===========================
# Data Loading Functions (MinIO)
# ===========================

@st.cache_data
def load_merged_data():
    """Loads merged_data.parquet from MinIO Gold Bucket"""
    # UPDATED PATH: merged_data subfolder
    path = "gold/merged_data/merged_data.parquet"
    try:
        # We use read_parquet because Phase 4 saved it as Parquet
        return pd.read_parquet(f"s3://{path}", storage_options=MINIO_OPTS)
    except Exception as e:
        st.error(f"Error loading {path} from MinIO: {e}")
        return None

@st.cache_data
def load_simulation_data():
    """Loads simulation_summary.csv from MinIO Gold Bucket"""
    # UPDATED PATH: monte_carlo subfolder
    path = "gold/monte_carlo/simulation_summary.csv"
    try:
        return pd.read_csv(f"s3://{path}", storage_options=MINIO_OPTS)
    except Exception as e:
        st.warning(f"Warning: Could not load {path}: {e}")
        return None

# Load Data
df = load_merged_data()
sim_df = load_simulation_data()

if df is None:
    st.error("Failed to load Merged Data from MinIO. Please ensure Phase 4 was successful.")
    st.stop()

# ===========================
# Sidebar: Filters
# ===========================
st.sidebar.header("Filters & Options")

# Dynamic Filter: Season (if exists)
if "season" in df.columns:
    seasons = ["All"] + sorted(df["season"].dropna().unique().tolist())
    sel_season = st.sidebar.selectbox("Filter by Season", seasons, index=0)
else:
    sel_season = "All"

# Dynamic Filter: Area 
if "area" in df.columns:
    areas = ["All"] + sorted(df["area"].dropna().unique().tolist())
    sel_area = st.sidebar.selectbox("Filter by Area", areas, index=0)
else:
    # Fallback if 'area' column is missing, just show All
    sel_area = "All"
    if "city" not in df.columns: # Only warn if neither exists
         st.sidebar.warning("Column 'area' not found in dataset.")

# Filter Logic
df_filtered = df.copy()

if sel_season != "All":
    df_filtered = df_filtered[df_filtered["season"] == sel_season]

if sel_area != "All":
    df_filtered = df_filtered[df_filtered["area"] == sel_area]

# Download Button (Convert filtered data to CSV for download)
@st.cache_data
def convert_df(df):
    return df.to_csv(index=False).encode('utf-8')

st.sidebar.markdown("---")
csv_data = convert_df(df_filtered)
st.sidebar.download_button(
    label="Download Filtered Data (CSV)",
    data=csv_data,
    file_name="filtered_traffic_data.csv",
    mime="text/csv",
)

# ===========================
# Main Tabs
# ===========================
tab_overview, tab_montecarlo, tab_factor = st.tabs(["📊 Overview", "🎲 Monte Carlo", "🔍 Factor Analysis"])

# ---------------------------
# Tab 1 — Overview
# ---------------------------
with tab_overview:
    st.header("Dataset Overview & Quick Metrics")

    # Metrics
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Records", f"{len(df_filtered):,}")
    
    if "vehicle_count" in df_filtered.columns:
        c2.metric("Total Vehicles", f"{int(df_filtered['vehicle_count'].sum()):,}")
    else:
        c2.metric("Total Vehicles", "N/A")
        
    if "accident_count" in df_filtered.columns:
        c3.metric("Total Accidents", f"{int(df_filtered['accident_count'].sum()):,}")
    else:
        c3.metric("Total Accidents", "N/A")

    # Sample Data
    st.markdown("### Sample Data (Top 10 Rows)")
    st.dataframe(df_filtered.head(10), use_container_width=True)

    # Correlation Heatmap
    st.markdown("### Correlation Heatmap (Numerical Features)")
    numeric_cols = df_filtered.select_dtypes(include=[np.number]).columns.tolist()
    
    # Remove ID columns that mess up correlation
    numeric_cols = [c for c in numeric_cols if "id" not in c.lower() and "unnamed" not in c.lower()]

    if len(numeric_cols) >= 2:
        fig, ax = plt.subplots(figsize=(10, 6))
        sns.heatmap(df_filtered[numeric_cols].corr(), annot=True, fmt=".2f", cmap="coolwarm", ax=ax)
        ax.set_title("Correlation Matrix")
        st.pyplot(fig, use_container_width=True)
    else:
        st.info("Not enough numerical columns for correlation matrix.")

    # Histogram
    st.markdown("### Distribution Plot")
    if numeric_cols:
        selected_col = st.selectbox("Choose column to plot:", numeric_cols)
        bins = st.slider("Number of Bins", 10, 200, 30)
        
        fig2, ax2 = plt.subplots(figsize=(8, 4))
        sns.histplot(df_filtered[selected_col].dropna(), bins=bins, kde=True, ax=ax2)
        ax2.set_xlabel(selected_col)
        ax2.set_ylabel("Count")
        st.pyplot(fig2, use_container_width=True)

# ---------------------------
# Tab 2 — Monte Carlo
# ---------------------------
with tab_montecarlo:
    st.header("Monte Carlo Simulation Results")
    
    if sim_df is None:
        st.warning("Simulation Summary not found in MinIO Gold bucket.")
    else:
        st.markdown("### Summary of Simulation Results")
        st.dataframe(sim_df, use_container_width=True)

        # Dynamic Column Selection for Plotting
        x_col = "Avg_Congestion_Prob" if "Avg_Congestion_Prob" in sim_df.columns else None
        y_col = "Scenario" if "Scenario" in sim_df.columns else None

        if x_col and y_col:
            st.markdown(f"### Risk Comparison: {x_col}")
            
            # Sort for better visualization
            sim_sorted = sim_df.sort_values(by=x_col, ascending=False)
            
            fig3, ax3 = plt.subplots(figsize=(10, 5))
            sns.barplot(data=sim_sorted, x=x_col, y=y_col, palette="magma", ax=ax3)
            ax3.set_xlabel("Probability / Risk")
            ax3.set_ylabel("Weather Scenario")
            st.pyplot(fig3, use_container_width=True)

            # Highlight Highest Risk
            top_row = sim_sorted.iloc[0]
            st.error(f"⚠️ Highest Risk Scenario: **{top_row[y_col]}** with Risk Score: {top_row[x_col]:.4f}")

# ---------------------------
# Tab 3 — Factor Analysis
# ---------------------------
with tab_factor:
    st.header("Interactive Factor Analysis")
    st.markdown("This tool calculates factors dynamically based on the filtered data above.")

    # Suggested Features from Phase 1 & 2
    suggested_features = [
        "vehicle_count", "avg_speed", "accident_count", 
        "temperature_c", "humidity", "rain_mm", 
        "wind_speed_kmh", "visibility_weather", "air_pressure_hpa"
    ]
    
    # Filter to only columns that actually exist in the dataframe
    available_features = [c for c in suggested_features if c in df_filtered.columns]

    if len(available_features) < 3:
        st.error("Not enough available features in the dataset for Factor Analysis.")
    else:
        st.markdown("### 1. Feature Selection")
        selected_features = st.multiselect(
            "Select features to analyze (Choose at least 3):", 
            available_features, 
            default=available_features[:6]
        )

        if len(selected_features) >= 3:
            # Prepare Data
            fa_df = df_filtered[selected_features].dropna()
            
            if len(fa_df) < 10:
                st.warning("Warning: Very low sample size after filtering.")
            
            st.write(f"Analyzing {fa_df.shape[0]} rows × {fa_df.shape[1]} columns.")

            # 1. Standardize Data (Crucial for FA)
            scaler = StandardScaler()
            X = scaler.fit_transform(fa_df)

            # 2. KMO / Bartlett Tests
            col_kmo, col_n = st.columns([1, 2])
            with col_kmo:
                if FACTOR_ANALYZER_AVAILABLE:
                    try:
                        _, kmo_model = calculate_kmo(fa_df)
                        st.info(f"**KMO Score:** {kmo_model:.3f}")
                        if kmo_model < 0.6:
                            st.caption("⚠️ Sampling is barely adequate.")
                        else:
                            st.caption("✅ Sampling is adequate.")
                    except:
                        st.warning("Could not calculate KMO.")
                else:
                    st.info("Install `factor_analyzer` to see KMO scores.")

            # 3. Factor Extraction (Sklearn)
            with col_n:
                n_factors = st.slider("Number of Factors to Extract:", 2, len(selected_features), 3)

            fa = FactorAnalysis(n_components=n_factors, random_state=42)
            fa.fit(X)

            # 4. Loadings DataFrame
            loadings = pd.DataFrame(
                fa.components_.T, 
                index=selected_features,
                columns=[f"Factor_{i+1}" for i in range(n_factors)]
            )

            # 5. Visualization
            st.subheader("Factor Loadings Heatmap")
            st.markdown("Strong colors indicate which variables belong to which factor.")
            
            fig4, ax4 = plt.subplots(figsize=(10, 6))
            sns.heatmap(loadings, annot=True, cmap="RdBu_r", center=0, ax=ax4)
            st.pyplot(fig4, use_container_width=True)

            # 6. Auto-Interpretation
            st.markdown("### Interpretation")
            for i in range(n_factors):
                colname = f"Factor_{i+1}"
                # Get top positive and negative associations
                top_pos = loadings[colname].nlargest(2).index.tolist()
                top_neg = loadings[colname].nsmallest(2).index.tolist()
                
                st.write(f"**{colname}**: dominated by `{', '.join(top_pos)}` (High) vs `{', '.join(top_neg)}` (Low)")

            # Download Results
            st.download_button(
                "Download Loadings Matrix", 
                loadings.to_csv().encode('utf-8'), 
                "factor_loadings.csv", 
                "text/csv"
            )

        else:
            st.info("Please select at least 3 features to begin analysis.")

# Footer
st.markdown("---")
st.markdown("✅ **System Status:** Connected to MinIO Gold Layer.")