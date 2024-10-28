import streamlit as st
from typing import Dict, Optional, List
from datetime import datetime
import logging
import plotly.graph_objects as go
import os
import sys

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../../"))
)

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
            initial_sidebar_state="expanded",
        )

    def setup_sidebar(self) -> bool:
        """Setup sidebar controls."""
        with st.sidebar:
            st.title("Contrôles du Dashboard")
            st.divider()

            use_simulation = st.toggle(
                "Utiliser des données simulées",
                value=False,
                help="Basculer entre les données simulées et réelles",
            )

        return use_simulation

    def fetch_data(
        self, endpoint_key: str, use_simulation: bool, params: Optional[Dict] = None
    ) -> Optional[Dict]:
        """Unified data fetching method."""
        try:
            data = self.data_service.fetch_data(
                endpoint_key=endpoint_key, use_simulation=use_simulation, params=params
            )
            return data
        except Exception as e:
            logger.error(f"Error fetching data from {endpoint_key}: {str(e)}")
            st.error(f"Error fetching data: {str(e)}")
            return None

    def display_summary_section(self, use_simulation: bool):
        """Display summary section with metrics and boxplot."""
        st.header("📊 Métriques Générales")

        with st.expander("ℹ️ À propos des Métriques Générales"):
            st.markdown("""
            **Vue d'ensemble:**
            
            📊 **Statistiques Patients**
            - Nombre total de patients dans l'EDS
            - Répartition par catégorie :
            * Patients Test (utilisés pour la validation)
            * Patients Recherche (inclus dans des protocoles)
            * Patients Sensibles (VIP, personnel hospitalier...)
            
            📈 **Statistiques Documents**
            - Volume total des documents stockés
            - Documents importés dans les 7 derniers jours
            
            ⏱️ **Délais de Traitement**
            - Mesure le temps écoulé entre :
            * Date de création : quand le document a été créé dans le système source
            * Date d'importation : quand le document a été intégré dans l'EDS
            - Permet d'évaluer la fraîcheur des données et l'efficacité des connecteurs
            - Les statistiques incluent :
            * Délai moyen et médian
            * Distribution (Q1, Q3, Min, Max)
            * Identification des retards potentiels
            """)

        # Fetch both summary and metrics data
        # Updated endpoint keys
        summary = self.fetch_data("summary", use_simulation)
        metrics = self.fetch_data("document_metrics", use_simulation)

        if summary:
            self.metrics_display.display_summary_metrics(summary)

        if metrics:
            self.display_boxplot(metrics)

    def display_boxplot(self, metrics: Dict[str, float]):
        """Display boxplot of document processing delays."""
        st.subheader("📈 Distribution des Délais de Traitement")

        # Create figure
        fig = go.Figure()

        # Add boxplot
        fig.add_trace(
            go.Box(
                q1=[metrics["q1"]],
                median=[metrics["median"]],
                q3=[metrics["q3"]],
                lowerfence=[metrics["min_delay"]],
                upperfence=[metrics["max_delay"]],
                mean=[metrics["avg_delay"]],
                name="Délais de Traitement",
                marker_color="rgb(8,81,156)",
                boxmean=True,
            )
        )

        # Customize layout
        fig.update_layout(
            title={
                "text": "Distribution des Délais de Traitement des Documents",
                "y": 0.95,
                "x": 0.5,
                "xanchor": "center",
                "yanchor": "top",
            },
            yaxis_title="Jours",
            showlegend=False,
            height=400,
            margin=dict(l=40, r=40, t=40, b=40),
            yaxis=dict(gridcolor="rgb(230,230,230)", zerolinecolor="rgb(200,200,200)"),
        )

        # Create two columns for visualization and metrics
        col1, col2 = st.columns([3, 1])

        with col1:
            # Display the plotly chart
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            # Display metrics in a clean format
            st.write("#### Statistiques Clés")
            metrics_display = {
                "Moyenne": f"{metrics['avg_delay']:.1f} jours",
                "Médiane": f"{metrics['median']:.1f} jours",
                "Q1 - Q3": f"{metrics['q1']:.1f} - {metrics['q3']:.1f} jours",
                "Min - Max": f"{metrics['min_delay']:.1f} - {metrics['max_delay']:.1f} jours",
            }

            for label, value in metrics_display.items():
                st.metric(label, value)

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

        doc_counts = self.fetch_data("document_counts", use_simulation)
        recent_doc_counts = self.fetch_data("recent_document_counts", use_simulation)

        if doc_counts or recent_doc_counts:
            tab1, tab2 = st.tabs(["Historique Complet", "Documents Récents"])

            with tab1:
                if doc_counts:
                    self.chart_display.create_document_distribution_chart(
                        doc_counts, "Distribution des Documents par Origine"
                    )

            with tab2:
                if recent_doc_counts:
                    self.chart_display.create_document_distribution_chart(
                        recent_doc_counts,
                        "Distribution des Documents Récents par Origine",
                    )

    def display_connector_monitoring(self, use_simulation: bool):
        """Display connector monitoring section."""
        try:
            st.header("📈 Monitoring des connecteurs")

            with st.expander("ℹ️ À propos du Monitoring des connecteurs"):
                st.markdown("""
                # ... [keep existing markdown] ...
                """)

            # Get all available origins directly from your API endpoint
            origin_codes = [
                "BIO",
                "DOC_EXTERNE_DIA",
                "CYBERLAB",
                "Easily_SOF",
                "DOC_EXTERNE_Car",
                "FOCH_EFR",
                "DOC_EXTERNE_Ari",
                "RDV_DOCTOLIB",
                "Easily_DIA",
                "DOC_EXTERNE_COP",
                "Easily_Car",
                "DOC_EXTERNE_Med",
                "Easily_echo_cardio",
                "Easily_COP",
                "Easily_EFR",
                "Easily_Patientys",
                "Easily_Muse",
                "Easily_Med",
                "Easily_CeS",
                "DOC_EXTERNE_PCA",
            ]

            # Initialize session state for selected origins
            if "selected_origins" not in st.session_state:
                # Take first 5 origins as default
                st.session_state.selected_origins = (
                    origin_codes[:5] if len(origin_codes) > 5 else origin_codes
                )
                print("\n=== Debug: Initial Session State ===")
                print(
                    f"Initialized selected_origins: {st.session_state.selected_origins}"
                )

            if "select_all" not in st.session_state:
                st.session_state.select_all = False

            def handle_select_all():
                st.session_state.selected_origins = origin_codes.copy()
                st.session_state.select_all = True
                print("\n=== Debug: Handle Select All ===")
                print(f"Updated selected_origins: {st.session_state.selected_origins}")

            def handle_selection_change():
                st.session_state.selected_origins = st.session_state.multiselect_value
                st.session_state.select_all = False
                print("\n=== Debug: Handle Selection Change ===")
                print(f"Updated selected_origins: {st.session_state.selected_origins}")

            col1, col2 = st.columns([3, 1])

            with col1:
                selected = st.multiselect(
                    "Sélectionner les Origines de Documents à Afficher",
                    options=origin_codes,
                    default=[
                        code
                        for code in st.session_state.selected_origins
                        if code in origin_codes
                    ],
                    key="multiselect_value",
                    help="Choisir les origines de documents à afficher dans les graphiques",
                )

            with col2:
                st.button("Tout Sélectionner", on_click=handle_select_all)

            if selected:
                self.display_time_series_data(selected, use_simulation)
            else:
                st.info("Veuillez sélectionner au moins une origine de documents.")

        except Exception as e:
            print(f"\n=== Debug: Error in display_connector_monitoring ===")
            print(f"Error type: {type(e).__name__}")
            print(f"Error message: {str(e)}")
            st.error(f"Une erreur s'est produite : {str(e)}")
            logger.error(f"Error in display_connector_monitoring: {str(e)}")

    def display_time_series_data(
        self, selected_origins: List[str], use_simulation: bool
    ):
        """Display time series data for selected origins."""
        try:
            # Simply join the selected origins with comma
            origin_codes_str = ",".join(selected_origins)

            print("\n=== Debug: Time Series Data Request ===")
            print(f"Selected origins: {selected_origins}")
            print(f"Origin codes string: {origin_codes_str}")

            params = {"origin_codes": origin_codes_str}

            with st.spinner("Chargement des données..."):
                yearly_data = self.fetch_data(
                    "document_counts_by_year", use_simulation, params=params
                )
                print(f"\nYearly data response: {yearly_data}")

                monthly_data = self.fetch_data(
                    "recent_document_counts_by_month", use_simulation, params=params
                )
                print(f"\nMonthly data response: {monthly_data}")

            if yearly_data and monthly_data:
                tab1, tab2 = st.tabs(["Tendance Annuelle", "Tendance Mensuelle"])

                with tab1:
                    if yearly_data:
                        self.chart_display.create_time_series_chart(
                            yearly_data,
                            "year",
                            "Nombre de Documents par Année",
                            show_range_selector=False,
                        )
                    else:
                        st.info("Aucune donnée annuelle disponible.")

                with tab2:
                    if monthly_data:
                        self.chart_display.create_time_series_chart(
                            monthly_data,
                            "month",
                            "Nombre de Documents Récents par Mois",
                            show_range_selector=True,
                        )
                    else:
                        st.info("Aucune donnée mensuelle disponible.")
            else:
                st.warning("Aucune donnée disponible pour les origines sélectionnées.")

        except Exception as e:
            print(f"\n=== Debug: Error in display_time_series_data ===")
            print(f"Error type: {type(e).__name__}")
            print(f"Error message: {str(e)}")
            st.error(f"Erreur lors de la récupération des données: {str(e)}")
            logger.error(f"Error in display_time_series_data: {str(e)}")

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

        top_users = self.fetch_data("top_users", use_simulation)

        current_year_users = self.fetch_data("top_users_current_year", use_simulation)

        if top_users or current_year_users:
            tab1, tab2 = st.tabs(["Historique Complet", "Année en Cours"])

            with tab1:
                if top_users:
                    self.chart_display.create_user_activity_chart(
                        top_users,
                        "Top Utilisateurs par Nombre de Requêtes (Historique)",
                    )

            with tab2:
                if current_year_users:
                    self.chart_display.create_user_activity_chart(
                        current_year_users,
                        f"Top Utilisateurs par Nombre de Requêtes ({datetime.now().year})",
                    )

    def display_archive_status(self, use_simulation: bool):
        """Display archive status section."""
        st.header("🗄️ Statut d'Archivage")
        with st.expander("ℹ️ À propos du Statut d'Archivage"):
            st.markdown("""
            **Période d'Archive:**
            - Calculée depuis la date de création du document (DOCUMENT_DATE) du plus ancien document
            - Les documents de plus de 20 ans sont candidats à l'archivage/suppression
            """)

        archive_data = self.fetch_data("archive_status", use_simulation)
        if archive_data:
            self.metrics_display.display_archive_metrics(archive_data)
            self.chart_display.create_archive_chart(archive_data)

    def display_pmsi(self, use_simulation: bool):
        """Display archive status section."""
        st.header("📋 Documents PMSI")
        with st.expander("ℹ️ À propos de l'onglet PMSI"):
            st.markdown("""
            Add Markdown
            """)

        # pmsi = self.fetch_data("pmsi", use_simulation)
        # if pmsi:
        #     self.metrics_display.display_archive_metrics(pmsi)
        #     self.chart_display.create_archive_chart(pmsi)

    def display_about_section(self, use_simulation: bool = None):
        """
        Display the about/home section.

        Args:
            use_simulation (bool, optional): Not used in this section but kept for consistency
        """
        # Centered welcome message with custom styling
        st.markdown(
            "<h1 style='text-align: center; color: #0f52ba;'>👋 Bienvenue sur le Dashboard EDS</h1>",
            unsafe_allow_html=True,
        )

        # Subtitle
        st.markdown(
            "<p style='text-align: center; font-size: 1.2em; color: #666;'>Monitoring de l'Entrepôt de Données de Santé</p>",
            unsafe_allow_html=True,
        )

        # Separator
        st.markdown("<hr style='margin: 2em 0;'>", unsafe_allow_html=True)

        # Brief introduction
        st.markdown(
            """
            <div style='text-align: center; margin-bottom: 2em;'>
            Ce tableau de bord fournit une analyse détaillée et en temps réel de l'Entrepôt de Données de Santé (EDS).
            Explorez les différentes sections pour obtenir des insights sur les données, les utilisateurs et les performances.
            </div>
        """,
            unsafe_allow_html=True,
        )

        # Create two columns for the sections
        col1, col2 = st.columns(2)

        with col1:
            st.markdown(
                """
                ### 📊 Métriques Générales
                <div style='background-color: #f0f2f6; padding: 1em; border-radius: 10px; margin-bottom: 1em;'>
                ✦ Nombre total de patients dans l'EDS<br>
                ✦ Décompte des patients test, recherche et sensibles<br>
                ✦ Vue d'ensemble du volume documentaire<br>
                ✦ Délai de distribution d'acheminement des documents
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
            """,
                unsafe_allow_html=True,
            )

        with col2:
            st.markdown(
                """
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
                
                ### 📋 Documents PMSI
                <div style='background-color: #f0f2f6; padding: 1em; border-radius: 10px; margin-bottom: 1em;'>
                ✦ Date du dernier chargement PMSI<br>
                ✦ Périodes concernées<br>
                ✦ Graphique linéaire des dates de création de documents PMSI<br>
                </div>
                
            """,
                unsafe_allow_html=True,
            )

        # Footer with update information
        st.markdown(
            """
            <div style='text-align: center; margin-top: 2em; padding: 1em; background-color: #e6f3ff; border-radius: 10px;'>
            ℹ️ <i>Ce tableau de bord est mis à jour en temps réel pour fournir une vision actualisée de l'état de l'EDS.</i>
            </div>
        """,
            unsafe_allow_html=True,
        )

        # Add some space at the bottom
        st.markdown("<br><br>", unsafe_allow_html=True)

    def run(self):
        """Run the dashboard application."""
        self.setup_page_config()

        st.title("Monitoring de l'Entrepôt de Donnée de Santé")
        st.caption("Vue d'ensemble complète des indicateurs de la base de données")

        use_simulation = self.setup_sidebar()

        # Initialize current section in session state if not exists
        if "current_section" not in st.session_state:
            st.session_state.current_section = "👋 Accueil"

        # Summary navigation
        st.sidebar.header("Navigation")
        navigation_options = {
            "👋 Accueil": self.display_about_section,
            "📊 Métriques Générales": self.display_summary_section,
            "📑 Distribution des Documents": self.display_document_distribution,
            "📈 Monitoring des Connecteurs": self.display_connector_monitoring,
            "👥 Activité Utilisateurs": self.display_user_activity,
            "🗄️ Statut d'Archivage": self.display_archive_status,
            "📋 Documents PMSI": self.display_pmsi,
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
