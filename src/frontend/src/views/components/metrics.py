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
