import streamlit as st
import pandas as pd
from src.database import engine
from sqlalchemy import text

## 1. Page Configuration and UI Branding ##

# st.set_page_config must be the first Streamlist command.
# "layout = 'wide'" utilizes the full screen width, which is the standard for data-heavy financial terminals (Bloomverg/Eikon style).
st.set_page_config(
    page_title="AlphaStream Quant Terminal", page_icon="📈", layout="wide"
)

st.title("📈 AlphaStream Quant Terminal")
st.markdown("""This dashboard visualizes the output of the **AlphaStream Data Pipeline**. It integrates live database records with mathematical risk metrics.""")
st.markdown("---")

## 2. Data Abstraction Layer ##

def get_dashboard_data():
    # Fetches the latest analytical signals from the SQL database. The system uses a "JOIN" operation to combine the human-readable ticker name (assets table) with the mathematical results (signals table).

    query = """
    SELECT
        a.ticker,
        s.price_date,
        s.signal_type,
        s.sharpe_ratio
    FROM assets a
    JOIN signals s ON a.asset_id = s.asset_id
    ORDER BY s.sharpe_ratio DESC -- Professional practice: show best assets first
    """
    try:
        with engine.connect() as conn:
            # Wrap the string in sqlalchemy.text() for security (preventing SQL injections)
            return pd.read_sql(text(query), conn)
    except Exception as e:
        st.error(f"Failed to connect to the database: {e}")
        return pd.DataFrame()

# Execute the data pull
df = get_dashboard_data()

## 3. Executive Summary (Top Metrics) ##

if not df.empty:
    # Split the screen into 3 columns for quick "At-a-glance" analysis.
    col1, col2, col3 = st.columns(3)

    with col1:
        # Total universe of stocks being tracked by the ETL script.
        st.metric("Assets Tracked", len(df))
    
    with col2:
        # The asset currently providing the highest risk-adjusted return.
        best_stock = df.iloc[0]["ticker"]
        st.metric("Top Sharpe Asset", best_stock)

    with col3:
        # Percentage of the portfolio currently in a "Trend Following" BUY state.
        buy_signals = len(df[df["signal_type"] == "BUY"])
        st.metric("Active BUY Signals", buy_signals)
    
    st.markdown("### 📊 Market Intelligence Table")

    ## 4. Conditional Formatting (Signal Visualization) ##

    def highlight_signals(val):
        # Applies visual "Heatmap" logic to the table.
        # Green signifies a trend-following BUY signal.
        # Yellow signifies a HOLD (neutral/defensive) state.
        if val == "BUY":
            color = "#2ecc71" # Professional green
        elif val == "HOLD":
            color = "#flc40f" # Professional yellow
        else:
            color = "transparent"
        return f"background-color: {color}; color: black; font-weight: bold"
    
    # Display the table with the visual stying applied to the "signal_type" column.
    st.dataframe(
        df.style.applymap(highlight_signals, subset=["signal_type"]), use_container_width=True
    )

    ## 5. Data Visualization (Charts) ##

    st.markdown("### Sharpe Ratio Comparison")
    # Bar charts allow the user to immediately identify outliers in risk performance.
    st.bar_chart(data=df, x="ticker", y="sharpe_ratio")

    # Educational Footer for stakeholders.
    st.info("""**Quant Insight:** The Sharpe Ratio measures the excess return per unit of deviation. 
            A ratio above 1.0 indicates that the asset's returns justify its volatility. 
            Ratios above 2.0 are considered excellent.
            """)
else:
    st.warning("No data found. Please ensure the ETL and Analytics scripts have been executed.")