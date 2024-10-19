import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
from requests.exceptions import RequestException
from json.decoder import JSONDecodeError

BASE_URL = "http://localhost:8000/api/v1"

def fetch_data(endpoint):
    try:
        response = requests.get(f"{BASE_URL}{endpoint}")
        response.raise_for_status()  # Raises an HTTPError for bad responses
        return response.json()
    except RequestException as e:
        st.error(f"Error fetching data from {endpoint}: {str(e)}")
        return None
    except JSONDecodeError:
        st.error(f"Error decoding JSON from {endpoint}. The response might be empty or not in JSON format.")
        return None

def safe_metric_display(label, value_func):
    try:
        value = value_func()
        st.metric(label, value)
    except Exception as e:
        st.metric(label, "N/A")
        st.error(f"Error displaying {label}: {str(e)}")

def safe_plot(plot_func):
    try:
        fig = plot_func()
        st.plotly_chart(fig)
    except Exception as e:
        st.error(f"Error creating plot: {str(e)}")

def main():
    st.set_page_config(page_title="Database Quality Dashboard", layout="wide")
    st.title("Database Quality Dashboard")

    if st.button("Refresh Data"):
        st.experimental_rerun()

    # Summary
    st.header("Summary")
    summary = fetch_data("/summary/api/summary")
    if summary:
        col1, col2, col3 = st.columns(3)
        with col1:
            safe_metric_display("Total Patients", lambda: f"{summary['patient_count']:,}")
        with col2:
            safe_metric_display("Total Documents", lambda: f"{summary['total_documents']:,}")
        with col3:
            safe_metric_display("Recent Documents (7 days)", lambda: f"{summary['recent_documents']:,}")
    else:
        st.warning("Summary data is not available.")

    # Users
    st.header("Top Users")
    top_users = fetch_data("/users/api/top_users")
    if top_users:
        df_top_users = pd.DataFrame(top_users, columns=["First Name", "Last Name", "Query Count"])
        safe_plot(lambda: px.bar(df_top_users, x="Last Name", y="Query Count", title="Top Users by Query Count"))
    else:
        st.warning("Top users data is not available.")

    st.subheader("Top Users (Current Year)")
    top_users_current_year = fetch_data("/users/api/top_users_current_year")
    if top_users_current_year:
        df_top_users_current_year = pd.DataFrame(top_users_current_year, columns=["First Name", "Last Name", "Query Count"])
        safe_plot(lambda: px.bar(df_top_users_current_year, x="Last Name", y="Query Count", title="Top Users by Query Count (Current Year)"))
    else:
        st.warning("Top users (current year) data is not available.")

    # Documents
    st.header("Document Metrics")
    doc_metrics = fetch_data("/documents/api/document_metrics")
    if doc_metrics:
        safe_plot(lambda: go.Figure(data=[go.Box(
            q1=[doc_metrics['q1']], median=[doc_metrics['median']],
            q3=[doc_metrics['q3']], lowerfence=[doc_metrics['min_delay']],
            upperfence=[doc_metrics['max_delay']], mean=[doc_metrics['avg_delay']],
            y=['Delay'],
            boxpoints='all',
            jitter=0.3,
            pointpos=-1.8
        )]).update_layout(title_text="Document Delay Distribution (in days)"))
    else:
        st.warning("Document metrics are not available.")

    st.subheader("Document Counts")
    doc_counts = fetch_data("/documents/api/document_counts")
    if doc_counts:
        df_doc_counts = pd.DataFrame(doc_counts, columns=["Origin", "Count"])
        safe_plot(lambda: px.pie(df_doc_counts, values="Count", names="Origin", title="Document Distribution by Origin"))
    else:
        st.warning("Document counts are not available.")

    st.subheader("Recent Document Counts")
    recent_doc_counts = fetch_data("/documents/api/recent_document_counts")
    if recent_doc_counts:
        df_recent_doc_counts = pd.DataFrame(recent_doc_counts, columns=["Origin", "Count"])
        safe_plot(lambda: px.pie(df_recent_doc_counts, values="Count", names="Origin", title="Recent Document Distribution by Origin"))
    else:
        st.warning("Recent document counts are not available.")

    # Sources
    st.header("Document Sources")
    if doc_counts:
        origin_codes = [origin for origin, _ in doc_counts]
        selected_origin = st.selectbox("Select Document Origin", origin_codes)

        st.subheader(f"Document Counts by Year - {selected_origin}")
        doc_counts_by_year = fetch_data(f"/sources/api/document_counts_by_year/{selected_origin}")
        if doc_counts_by_year:
            df_doc_counts_by_year = pd.DataFrame(doc_counts_by_year, columns=["Year", "Count"])
            safe_plot(lambda: px.line(df_doc_counts_by_year, x="Year", y="Count", title=f"Document Counts by Year - {selected_origin}"))
        else:
            st.warning(f"Document counts by year for {selected_origin} are not available.")

        st.subheader(f"Recent Document Counts by Month - {selected_origin}")
        recent_doc_counts_by_month = fetch_data(f"/sources/api/recent_document_counts_by_month/{selected_origin}")
        if recent_doc_counts_by_month:
            df_recent_doc_counts_by_month = pd.DataFrame(recent_doc_counts_by_month, columns=["Month", "Count"])
            safe_plot(lambda: px.line(df_recent_doc_counts_by_month, x="Month", y="Count", title=f"Recent Document Counts by Month - {selected_origin}"))
        else:
            st.warning(f"Recent document counts by month for {selected_origin} are not available.")
    else:
        st.warning("Document sources data is not available.")

    # Archives
    st.header("Archive Status")
    archive_status = fetch_data("/archives/api/archive_status")
    if archive_status:
        safe_metric_display("Archive Period (years)", lambda: f"{archive_status['archive_period']:.2f}")
        safe_metric_display("Total Documents to Suppress", lambda: f"{archive_status['total_documents_to_suppress']:,}")

        if 'documents_to_suppress' in archive_status:
            df_docs_to_suppress = pd.DataFrame(archive_status['documents_to_suppress'], columns=["Origin", "Count"])
            safe_plot(lambda: px.bar(df_docs_to_suppress, x="Origin", y="Count", title="Documents to Suppress by Origin"))
        else:
            st.warning("Documents to suppress data is not available.")
    else:
        st.warning("Archive status data is not available.")

if __name__ == "__main__":
    main()