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
from src.frontend.src.services.data_service import DataService
from src.frontend.src.views.components.metrics import MetricsDisplay
from src.frontend.src.views.components.charts import ChartDisplay



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
                    
            # with st.expander("À propos"):
            #     st.write("""
            #     Ce tableau de bord fournit une analyse en temps réel de la base de données, 
            #     comprenant les indicateurs de qualité, les tendances d'utilisation, 
            #     et la répartition des documents.
            #     """)
            
            with st.expander("À propos"):
              st.markdown("""
                ### Tableau de Bord de Monitoring EDS
                
                Ce tableau de bord fournit une analyse détaillée de l'Entrepôt de Données de Santé (EDS) avec les sections suivantes :
                
                #### 📊 Métriques Générales
                - Nombre total de patients dans l'EDS
                - Décompte des patients test, recherche et sensibles 
                - Vue d'ensemble du volume documentaire
                
                #### 📑 Distribution des Documents
                - Répartition par origine des documents
                - Comparaison historique vs récent
                - Analyse des tendances documentaires
                
                #### 📈 Monitoring des Connecteurs  
                - Évolution annuelle du volume par connecteur
                - Tendances mensuelles détaillées
                - Performance des imports de données
                
                #### 👥 Activité Utilisateurs
                - Top utilisateurs par nombre de requêtes
                - Utilisation historique vs année en cours
                - Répartition des accès
                
                #### 🗄️ Statut d'Archivage
                - Période d'archivage globale
                - Documents éligibles à l'archivage
                - Distribution par type de document
                
                *Ce tableau de bord est mis à jour en temps réel pour fournir une vision actualisée de l'état de l'EDS.*
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
            ...
            """)

        doc_counts = self.fetch_data_with_simulation("/api/document_counts", use_simulation)
        if doc_counts:
            origin_codes = sorted(set(item["document_origin_code"] for item in doc_counts))

            # Initialize session state for both the selection and the "select all" state
            if "selected_origins" not in st.session_state:
                st.session_state.selected_origins = origin_codes[:5] if len(origin_codes) > 5 else origin_codes
            if "select_all" not in st.session_state:
                st.session_state.select_all = False

            # Create callback functions to handle state changes
            def handle_select_all():
                st.session_state.selected_origins = origin_codes
                st.session_state.select_all = True

            def handle_selection_change():
                # Update selected_origins based on the multiselect value
                st.session_state.selected_origins = st.session_state.multiselect_value
                st.session_state.select_all = False

            col1, col2 = st.columns([3, 1])
            
            with col1:
                # Use the session state value as the default and store new selections in session state
                st.session_state.selected_origins = st.multiselect(
                    "Sélectionner les Origines de Documents à Afficher",
                    options=origin_codes,
                    default=st.session_state.selected_origins,
                    key="multiselect_value",
                    on_change=handle_selection_change,
                    help="Choisir les origines de documents à afficher dans les graphiques"
                )

            with col2:
                if st.button("Tout Sélectionner", on_click=handle_select_all):
                    pass  # The actual selection is handled in the callback

            # Display the time series data using the session state values
            if st.session_state.selected_origins:
                self.display_time_series_data(st.session_state.selected_origins, use_simulation)



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
        

    def display_about_section(self, use_simulation: bool = None):
        """
        Display the about/home section.
        
        Args:
            use_simulation (bool, optional): Not used in this section but kept for consistency
        """
        # Centered welcome message with custom styling
        st.markdown("<h1 style='text-align: center; color: #0f52ba;'>👋 Bienvenue sur le Dashboard EDS</h1>", unsafe_allow_html=True)
        
        # Subtitle
        st.markdown("<p style='text-align: center; font-size: 1.2em; color: #666;'>Monitoring de l'Entrepôt de Données de Santé</p>", unsafe_allow_html=True)
        
        # Separator
        st.markdown("<hr style='margin: 2em 0;'>", unsafe_allow_html=True)

        # Brief introduction
        st.markdown("""
            <div style='text-align: center; margin-bottom: 2em;'>
            Ce tableau de bord fournit une analyse détaillée et en temps réel de l'Entrepôt de Données de Santé (EDS).
            Explorez les différentes sections pour obtenir des insights sur les données, les utilisateurs et les performances.
            </div>
        """, unsafe_allow_html=True)

        # Create two columns for the sections
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("""
                ### 📊 Métriques Générales
                <div style='background-color: #f0f2f6; padding: 1em; border-radius: 10px; margin-bottom: 1em;'>
                ✦ Nombre total de patients dans l'EDS<br>
                ✦ Décompte des patients test, recherche et sensibles<br>
                ✦ Vue d'ensemble du volume documentaire
                </div>

                ### 📈 Monitoring des Connecteurs
                <div style='background-color: #f0f2f6; padding: 1em; border-radius: 10px; margin-bottom: 1em;'>
                ✦ Évolution annuelle du volume par connecteur<br>
                ✦ Tendances mensuelles détaillées<br>
                ✦ Performance des imports de données
                </div>

                ### 🗄️ Statut d'Archivage
                <div style='background-color: #f0f2f6; padding: 1em; border-radius: 10px; margin-bottom: 1em;'>
                ✦ Période d'archivage globale<br>
                ✦ Documents éligibles à l'archivage<br>
                ✦ Distribution par type de document
                </div>
            """, unsafe_allow_html=True)

        with col2:
            st.markdown("""
                ### 📑 Distribution des Documents
                <div style='background-color: #f0f2f6; padding: 1em; border-radius: 10px; margin-bottom: 1em;'>
                ✦ Répartition par origine des documents<br>
                ✦ Comparaison historique vs récent<br>
                ✦ Analyse des tendances documentaires
                </div>

                ### 👥 Activité Utilisateurs
                <div style='background-color: #f0f2f6; padding: 1em; border-radius: 10px; margin-bottom: 1em;'>
                ✦ Top utilisateurs par nombre de requêtes<br>
                ✦ Utilisation historique vs année en cours<br>
                ✦ Répartition des accès
                </div>
            """, unsafe_allow_html=True)

        # Footer with update information
        st.markdown("""
            <div style='text-align: center; margin-top: 2em; padding: 1em; background-color: #e6f3ff; border-radius: 10px;'>
            ℹ️ <i>Ce tableau de bord est mis à jour en temps réel pour fournir une vision actualisée de l'état de l'EDS.</i>
            </div>
        """, unsafe_allow_html=True)

        # Add some space at the bottom
        st.markdown("<br><br>", unsafe_allow_html=True)


    def run(self):
        """Run the dashboard application."""
        self.setup_page_config()
        
        st.title("Monitoring de l'Entrepôt de Donnée de Santé")
        st.caption("Vue d'ensemble complète des indicateurs de la base de données")
        
        use_simulation = self.setup_sidebar()
        
        # Initialize current section in session state if not exists
        if 'current_section' not in st.session_state:
            st.session_state.current_section = "Accueil"

        # Summary navigation
        st.sidebar.header("Navigation")
        navigation_options = {
            "Accueil": self.display_about_section,
            "Métriques Générales": self.display_summary_section,
            "Distribution des Documents": self.display_document_distribution,
            "Monitoring des Connecteurs": self.display_connector_monitoring,
            "Activité Utilisateurs": self.display_user_activity,
            "Statut d'Archivage": self.display_archive_status
        }
        
        # Create navigation buttons
        for section_name, display_function in navigation_options.items():
            if st.sidebar.button(section_name, key=f"nav_{section_name}"):
                st.session_state.current_section = section_name

        # Display the current section
        if st.session_state.current_section in navigation_options:
            navigation_options[st.session_state.current_section](use_simulation)

def main():
    """Main entry point for the dashboard."""
    dashboard = Dashboard()
    dashboard.run()

if __name__ == "__main__":
    main()