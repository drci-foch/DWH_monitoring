# src/views/pages/dashboard.py
import streamlit as st
from typing import Dict, Optional, List
from datetime import datetime
import logging
import sys
import os
# Append one directory above 'src_api'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../../')))

# Try importing again
from src_api.frontend.src.services.data_service import DataService
from src_api.frontend.src.views.components.metrics import MetricsDisplay
from src_api.frontend.src.views.components.charts import ChartDisplay



logger = logging.getLogger(__name__)

class Dashboard:
    def __init__(self):
        """Initialize Dashboard with DataService and setup logging."""
        self.data_service = DataService()
        self.metrics_display = MetricsDisplay()
        self.chart_display = ChartDisplay()

    def setup_page_config(self):
        """Configure Streamlit page settings."""
        st.set_page_config(
            page_title="Monitoring de l'Entrepôt de Donnée de Santé - Base",
            layout="wide",
            initial_sidebar_state="expanded"
        )

    def setup_sidebar(self) -> bool:
        """
        Setup sidebar controls.
        
        Returns:
            bool: Whether to use simulated data
        """
        with st.sidebar:
            st.title("Contrôles du Dashboard")
            st.divider()
            
            use_simulation = st.toggle(
                "Utiliser des données simulées", 
                value=True,
                help="Basculer entre les données simulées et réelles"
            )
                    
            if st.button("🔄 Actualiser les données", type="primary", use_container_width=True):
                st.rerun()
                    
            with st.expander("À propos"):
                st.write("""
                Ce tableau de bord fournit une analyse en temps réel de la base de données, 
                comprenant les indicateurs de qualité, les tendances d'utilisation, 
                et la répartition des documents.
                """)
                
        return use_simulation

    def fetch_data_with_simulation(self, endpoint: str, use_simulation: bool, params: Optional[Dict] = None) -> Optional[Dict]:
        """
        Fetch data with simulation toggle.
        
        Args:
            endpoint (str): API endpoint
            use_simulation (bool): Whether to use simulated data
            params (Optional[Dict]): Optional parameters for the API call
            
        Returns:
            Optional[Dict]: The fetched data
        """
        try:
            if use_simulation:
                return self.data_service.fetch_simulated_data(endpoint, params)
            return self.data_service.fetch_data(endpoint, params)
        except Exception as e:
            logger.error(f"Error fetching data from {endpoint}: {str(e)}")
            st.error(f"Error fetching data: {str(e)}")
            return None

    def display_summary_section(self, use_simulation: bool):

            
        summary = self.fetch_data_with_simulation("/summary/api/summary", use_simulation)
        if summary:
            self.metrics_display.display_summary_metrics(summary)

    def display_document_distribution(self, use_simulation: bool):
        """Display document distribution section."""
        st.header("📑 Distribution des Documents")
        with st.expander("ℹ️ À propos de la Distribution des Documents"):
            st.markdown("""
            **Regroupement des Données:**
            - Les documents dont le code d'origine commence par 'Easily' sont regroupés sous 'Easily'
            - Les documents dont le code d'origine commence par 'DOC_EXTERNE' sont regroupés sous 'DOC_EXTERNE'
            - Les autres documents conservent leur code d'origine initial
            
            **Périodes:**
            - **Historique Complet**: Affiche tous les documents uniques sur l'ensemble des périodes
            - **Documents Récents**: Affiche les documents mis à jour ces 7 derniers jours
            
            Les documents sont comptabilisés en utilisant des DOCUMENT_NUM distincts pour éviter les doublons.
            """)
            
        doc_counts = self.fetch_data_with_simulation("/api/document_counts", use_simulation)
        recent_doc_counts = self.fetch_data_with_simulation("/api/recent_document_counts", use_simulation)
        
        if doc_counts or recent_doc_counts:
            tab1, tab2 = st.tabs(["Historique Complet", "Documents Récents"])
            
            with tab1:
                if doc_counts:
                    self.chart_display.create_document_distribution_chart(
                        doc_counts,
                        "Distribution des Documents par Origine"
                    )
            
            with tab2:
                if recent_doc_counts:
                    self.chart_display.create_document_distribution_chart(
                        recent_doc_counts,
                        "Distribution des Documents Récents par Origine"
                    )

    def display_connector_monitoring(self, use_simulation: bool):
        """Display connector monitoring section."""
        st.header("📈 Monitoring des connecteurs")
        with st.expander("ℹ️ À propos du Monitoring des connecteurs"):
            st.markdown("""
            **Tendances Annuelles:**
            - Affiche le nombre de documents groupés par année basé sur UPDATE_DATE
            - Inclut tous les documents pour les codes d'origine sélectionnés
            - Les décomptes sont basés sur des identifiants de documents distincts
            
            **Tendances Mensuelles:**
            - Affiche les données des 12 derniers mois
            - Basé sur DOCUMENT_DATE (date de création)
            - Filtré depuis le début de l'année précédente
            - Mise à jour mensuelle en début de mois
            - Exclut les dates futures
            """)

        doc_counts = self.fetch_data_with_simulation("/api/document_counts", use_simulation)
        if doc_counts:
            origin_codes = sorted(set(item["document_origin_code"] for item in doc_counts))
            
            col1, col2 = st.columns([3, 1])
            with col1:
                selected_origins = st.multiselect(
                    "Sélectionner les Origines de Documents à Afficher",
                    options=origin_codes,
                    default=origin_codes[:5] if len(origin_codes) > 5 else origin_codes,
                    help="Choisir les origines de documents à afficher dans les graphiques"
                )
            
            with col2:
                if st.button("Tout Sélectionner"):
                    selected_origins = origin_codes
            
            if selected_origins:
                self.display_time_series_data(selected_origins, use_simulation)

    def display_time_series_data(self, selected_origins: List[str], use_simulation: bool):
        """Display time series data for selected origins."""
        origin_codes_str = ','.join(selected_origins)  # Convert to comma-separated string
        yearly_data = self.fetch_data_with_simulation(
            "/sources/document_counts_by_year",
            use_simulation,
            {"origin_codes": origin_codes_str}
        )
        logger.debug(f"Yearly Data Retrieved: {yearly_data}")

        monthly_data = self.fetch_data_with_simulation(
            "api/v1/sources/recent_document_counts_by_month",
            use_simulation,
            {"origin_codes": origin_codes_str}
        )
        
        # Log the fetched data for debugging
        logger.debug(f"Yearly Data: {yearly_data}")
        logger.debug(f"Monthly Data: {monthly_data}")
        
        if yearly_data and monthly_data:
            tab1, tab2 = st.tabs(["Tendance Annuelle", "Tendance Mensuelle"])
            
            with tab1:
                self.chart_display.create_time_series_chart(
                    yearly_data,
                    "year",
                    "Nombre de Documents par Année",
                    show_range_selector=False
                )
            
            with tab2:
                self.chart_display.create_time_series_chart(
                    monthly_data,
                    "month",
                    "Nombre de Documents Récents par Mois",
                    show_range_selector=True
                )
        else:
            st.warning("No data available for the selected origins.")


    def display_user_activity(self, use_simulation: bool):
        """Display user activity section."""
        st.header("👥 Activité Utilisateurs")
        with st.expander("ℹ️ À propos de l'Activité Utilisateurs"):
            st.markdown("""
            **Analyse des Requêtes Utilisateurs:**
            - Affiche les 10 premiers utilisateurs par nombre de requêtes
            - Les membres de l'équipe CODOC sont regroupés sous 'CODOC'
            - Les comptages sont basés sur la table DWH_LOG_QUERY
            """)
        
        top_users = self.fetch_data_with_simulation("/api/top_users", use_simulation)
        current_year_users = self.fetch_data_with_simulation("/api/top_users_current_year", use_simulation)
        
        if top_users or current_year_users:
            tab1, tab2 = st.tabs(["Historique Complet", "Année en Cours"])
            
            with tab1:
                if top_users:
                    self.chart_display.create_user_activity_chart(
                        top_users,
                        "Top Utilisateurs par Nombre de Requêtes (Historique)"
                    )
            
            with tab2:
                if current_year_users:
                    self.chart_display.create_user_activity_chart(
                        current_year_users,
                        f"Top Utilisateurs par Nombre de Requêtes ({datetime.now().year})"
                    )

    def display_archive_status(self, use_simulation: bool):
        """Display archive status section."""
        st.header("🗄️ Statut d'Archivage")
        with st.expander("ℹ️ À propos du Statut d'Archivage"):
            st.markdown("""
            **Période d'Archive:**
            - Calculée depuis la date de mise à jour (UPDATE_DATE) du plus ancien document
            - Les documents de plus de 20 ans sont candidats à l'archivage/suppression
            """)
            
        archive_data = self.fetch_data_with_simulation("/archives/api/archive_status", use_simulation)
        if archive_data:
            self.metrics_display.display_archive_metrics(archive_data)
            self.chart_display.create_archive_chart(archive_data)
        


    def run(self):
        """Run the dashboard application."""
        self.setup_page_config()
        
        st.title("Monitoring de l'Entrepôt de Donnée de Santé")
        st.caption("Vue d'ensemble complète des indicateurs de la base de données")
        
   
        use_simulation = self.setup_sidebar()
        
        # Summary navigation
        st.sidebar.header("Navigation")
        navigation_options = {
            "Résumé": self.display_summary_section,
            "Distribution des Documents": self.display_document_distribution,
            "Monitoring des Connecteurs": self.display_connector_monitoring,
            "Activité Utilisateurs": self.display_user_activity,
            "Statut d'Archivage": self.display_archive_status
        }
        
        for section_name, display_function in navigation_options.items():
            if st.sidebar.button(section_name):
                display_function(use_simulation)

        # Display the initial section (Résumé)
        #self.display_summary_section(use_simulation)

def main():
    """Main entry point for the dashboard."""
    dashboard = Dashboard()
    dashboard.run()

if __name__ == "__main__":
    main()