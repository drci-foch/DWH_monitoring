import streamlit as st
from typing import List, Tuple, Dict, Any
from datetime import datetime
from datetime import timedelta


class MetricsDisplay:
    @staticmethod
    def create_metric_grid(metrics: List[Tuple[str, int, str]], num_columns: int = 6):
        """
        Create a grid of metrics in Streamlit.

        Args:
            metrics (List[Tuple[str, int, str]]): List of (label, value, help) tuples
            num_columns (int): Number of columns in the grid
        """
        cols = st.columns(num_columns)
        for idx, (label, value, help_text) in enumerate(metrics):
            with cols[idx % num_columns]:
                st.metric(label=label, value=f"{value:,}", help=help_text)

    @staticmethod
    def display_summary_metrics(summary: Dict[str, Any]):
        """
        Display the summary metrics section.

        Args:
            summary (Dict[str, Any]): Summary data containing patient and document counts
        """

        metrics = [
            (
                "👥 Total Patients",
                summary["patient_count"],
                "Nombre total de patients ayant au moins 1 document",
            ),
            (
                "🧪 Patients Test",
                summary["test_patient_count"],
                "Patients de test (Nom de famille = 'TEST')",
            ),
            (
                "🔬 Patients Recherche",
                summary["research_patient_count"],
                "Patients de recherche (Nom de famille = 'INSECTE')",
            ),
            (
                "⭐ Patients Sensibles",
                summary["celebrity_patient_count"],
                "Patients sensibles (Nom de famille = 'FLEUR')",
            ),
            (
                "📄 Documents Totaux",
                summary["total_documents"],
                "Nombre total de documents dans l'entrepôt",
            ),
            (
                "📥 Documents Récents",
                summary["recent_documents"],
                "Nombre de documents importés sur les 7 derniers jours",
            ),
        ]

        MetricsDisplay.create_metric_grid(metrics)

    @staticmethod
    def display_archive_metrics(archive_data: Dict[str, Any]):
        """
        Display the archive metrics section.

        Args:
            archive_data (Dict[str, Any]): Archive status data
        """
        st.subheader("Analyse de la Période d'Archive")

        period_cols = st.columns([2, 1, 1])
        current_date = datetime.now()
        oldest_date = current_date - timedelta(
            days=int(archive_data["archive_period"] * 365.25)
        )

        with period_cols[0]:
            st.metric(
                "Période d'Archive (années)",
                f"{archive_data['archive_period']:.1f}",
                help="Durée entre le document le plus ancien et la date actuelle",
            )

        with period_cols[1]:
            st.metric(
                "Date du Document le Plus Ancien",
                oldest_date.strftime("%Y-%m-%d"),
                help="Date du document le plus ancien dans le système",
            )

        with period_cols[2]:
            st.metric(
                "Seuil de Suppression",
                "20 ans",
                help="Les documents de plus de 20 ans (240 mois) sont marqués pour suppression",
            )

    @staticmethod
    def display_connector_statistics(yearly_data: List[Dict], monthly_data: List[Dict]):
        """Display statistical analysis for connectors."""
        import pandas as pd
        
        # Create DataFrames
        df_yearly = pd.DataFrame(yearly_data)
        df_monthly = pd.DataFrame(monthly_data)
        df_monthly['month'] = pd.to_datetime(df_monthly['month'])
        
        # Calculate statistics per connector
        stats_cols = st.columns(2)
        
        with stats_cols[0]:
            st.subheader("📊 Analyses Annuelles")
            
            # Calculate year-over-year growth
            for connector in df_yearly['document_origin_code'].unique():
                connector_data = df_yearly[df_yearly['document_origin_code'] == connector]
                growth = connector_data.set_index('year')['count'].pct_change() * 100
                total_docs = connector_data['count'].sum()
                avg_growth = growth.mean()
                
                with st.expander(f"🔗 {connector}"):
                    st.metric(
                        "Volume Total",
                        f"{total_docs:,} documents",
                        help="Nombre total de documents sur toute la période"
                    )
                    st.metric(
                        "Croissance Moyenne Annuelle",
                        f"{avg_growth:.1f}%",
                        help="Croissance moyenne d'une année sur l'autre"
                    )
                    
                    # # Show yearly progression
                    # yearly_counts = connector_data.set_index('year')['count']
                    # st.write("Progression annuelle:")
                    # for year, count in yearly_counts.items():
                    #     st.write(f"- {year}: {count:,} documents")
        
        with stats_cols[1]:
            st.subheader("📊 Analyse Mensuelle")
            
            # Calculate monthly statistics
            for connector in df_monthly['document_origin_code'].unique():
                connector_data = df_monthly[df_monthly['document_origin_code'] == connector]
                
                with st.expander(f"🔗 {connector}"):
                    monthly_avg = connector_data['count'].mean()
                    monthly_std = connector_data['count'].std()
                    peak_month = connector_data.loc[connector_data['count'].idxmax()]
                    low_month = connector_data.loc[connector_data['count'].idxmin()]
                    
                    st.metric(
                        "Moyenne Mensuelle",
                        f"{monthly_avg:.0f} documents",
                        help="Nombre moyen de documents ajoutés par mois"
                    )
                    st.metric(
                        "Écart-Type",
                        f"{monthly_std:.0f}",
                        help="Mesure de la variabilité mensuelle"
                    )
                    st.write(f"🔼 Pic: {peak_month['month'].strftime('%Y-%m')}: {peak_month['count']:,} documents")
                    st.write(f"⬇️ Minimum: {low_month['month'].strftime('%Y-%m')}: {low_month['count']:,} documents")
        
        # Add overall statistics
        st.subheader("📊 Vue d'Ensemble")
        total_cols = st.columns(3)
        
        with total_cols[0]:
            total_yearly = df_yearly['count'].sum()
            st.metric(
                "Volume Total",
                f"{total_yearly:,}",
                help="Nombre total de documents tous connecteurs confondus"
            )
            
        with total_cols[1]:
            avg_monthly = df_monthly.groupby('document_origin_code')['count'].mean().mean()
            st.metric(
                "Moyenne Mensuelle Globale",
                f"{avg_monthly:.0f}",
                help="Moyenne mensuelle de documents ajoutés sur tous connecteurs confondus"
            )
            
        with total_cols[2]:
            active_connectors = len(df_monthly['document_origin_code'].unique())
            st.metric(
                "Connecteurs Actifs",
                active_connectors,
                help="Nombre de connecteurs actifs sur les 12 derniers mois"
            )