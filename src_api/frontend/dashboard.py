import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
from requests.exceptions import RequestException
from json.decoder import JSONDecodeError
import logging

logging.basicConfig(level=logging.INFO)
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

def safe_plot(plot_function):
    try:
        fig = plot_function()
        st.plotly_chart(fig)
    except Exception as e:
        st.error(f"Error creating plot: {str(e)}")
        logging.error(f"Plot error: {str(e)}", exc_info=True)

def main():
    st.set_page_config(page_title="Database Quality Dashboard", layout="wide")
    st.title("Database Quality Dashboard")

    if st.button("Refresh Data"):
        st.rerun()

    # Summary
    st.header("Summary")
    summary = fetch_data("/summary/api/summary")
    if summary:
        col1, col2, col3, col4, col5, col6 = st.columns(6)
        with col1:
            safe_metric_display("Total Patients", lambda: f"{summary['patient_count']:,}")
        with col2:
            safe_metric_display("Total Test Patients", lambda: f"{summary['test_patient_count']:,}")
        with col3:
            safe_metric_display("Total Research Patients", lambda: f"{summary['research_patient_count']:,}")
        with col4:
            safe_metric_display("Total Celebrity Patients", lambda: f"{summary['celebrity_patient_count']:,}")
        with col5:
            safe_metric_display("Total Documents", lambda: f"{summary['total_documents']:,}")
        with col6:
            safe_metric_display("Recent Documents (7 days)", lambda: f"{summary['recent_documents']:,}")
    else:
        st.warning("Summary data is not available.")

    # Users
    st.header("Top Users")
    top_users = fetch_data("/users/api/top_users")
    if top_users:
        # Create DataFrame with correct column names
        df_top_users = pd.DataFrame(top_users)
        
        # Trim whitespace from names
        df_top_users['firstname'] = df_top_users['firstname'].str.strip()
        df_top_users['lastname'] = df_top_users['lastname'].str.strip()
        
        # Create plot
        safe_plot(lambda: px.bar(df_top_users, x="lastname", y="query_count", 
                                title="Top Users by Query Count",
                                labels={"lastname": "Last Name", "query_count": "Query Count"},
                                hover_data=["firstname"]))
    else:
        st.warning("Top users data is not available.")

    st.subheader("Top Users (Current Year)")
    top_users_current_year = fetch_data("/users/api/top_users_current_year")
    if top_users_current_year:
        # Create DataFrame with correct column names
        df_top_users_current_year = pd.DataFrame(top_users_current_year)
        
        # Trim whitespace from names
        df_top_users_current_year['firstname'] = df_top_users_current_year['firstname'].str.strip()
        df_top_users_current_year['lastname'] = df_top_users_current_year['lastname'].str.strip()
        
        # Create plot
        safe_plot(lambda: px.bar(df_top_users_current_year, x="lastname", y="query_count", 
                                title="Top Users by Query Count (Current Year)",
                                labels={"lastname": "Last Name", "query_count": "Query Count"},
                                hover_data=["firstname"]))
    else:
        st.warning("Top users (current year) data is not available.")

    st.subheader("Document Counts")
    doc_counts = fetch_data("/documents/api/document_counts")
    if doc_counts:
        df_doc_counts = pd.DataFrame(doc_counts)
        safe_plot(lambda: px.pie(df_doc_counts, 
                                values="unique_document_count", 
                                names="document_origin_code", 
                                title="Document Distribution by Origin"))
    else:
        st.warning("Document counts are not available.")

    st.subheader("Recent Document Counts")
    recent_doc_counts = fetch_data("/documents/api/recent_document_counts")
    if recent_doc_counts:
        df_recent_doc_counts = pd.DataFrame(recent_doc_counts)
        safe_plot(lambda: px.pie(df_recent_doc_counts, 
                                values="unique_document_count", 
                                names="document_origin_code", 
                                title="Recent Document Distribution by Origin"))
    else:
        st.warning("Recent document counts are not available.")
    # Sources



    # Fetch document counts
    doc_counts = fetch_data("/api/v1/documents/document_counts")

    if doc_counts:
        # Extract origin codes from doc_counts
        origin_codes = [item['document_origin_code'] for item in doc_counts]
        
        selected_origin = st.selectbox("Select Document Origin", origin_codes)
        
        st.subheader(f"Document Counts by Year - {selected_origin}")
        doc_counts_by_year = fetch_data(f"/api/v1/documents/document_counts_by_year/{selected_origin}")
        
        if doc_counts_by_year:
            df_doc_counts_by_year = pd.DataFrame(doc_counts_by_year)
            safe_plot(lambda: px.line(df_doc_counts_by_year, x="year", y="count", title=f"Document Counts by Year - {selected_origin}"))
        else:
            st.warning(f"Document counts by year for {selected_origin} are not available.")
        
        st.subheader(f"Recent Document Counts by Month - {selected_origin}")
        recent_doc_counts_by_month = fetch_data(f"/api/v1/documents/recent_document_counts_by_month/{selected_origin}")
        
        if recent_doc_counts_by_month:
            df_recent_doc_counts_by_month = pd.DataFrame(recent_doc_counts_by_month)
            safe_plot(lambda: px.line(df_recent_doc_counts_by_month, x="month", y="count", title=f"Recent Document Counts by Month - {selected_origin}"))
        else:
            st.warning(f"Recent document counts by month for {selected_origin} are not available.")
    else:
        st.warning("Document sources data is not available.")

    #     # Archives
    #     st.header("Archive Status")
        
    # archive_status = fetch_data("/archives/api/archive_status")
    # if archive_status:
    #     safe_metric_display("Archive Period (years)", lambda: f"{archive_status['archive_period']:.2f}")
    #     safe_metric_display("Total Documents to Suppress", lambda: f"{archive_status['total_documents_to_suppress']:,}")

    #     if 'documents_to_suppress' in archive_status:
    #         df_docs_to_suppress = pd.DataFrame(archive_status['documents_to_suppress'], columns=["Origin", "Count"])
    #         safe_plot(lambda: px.bar(df_docs_to_suppress, x="Origin", y="Count", title="Documents to Suppress by Origin"))
    #     else:
    #         st.warning("Documents to suppress data is not available.")
    # else:
    #     st.warning("Archive status data is not available.")

if __name__ == "__main__":
    main()