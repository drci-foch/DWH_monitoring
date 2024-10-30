import streamlit as st
from typing import Dict, Optional, List
from datetime import datetime
import logging
import plotly.graph_objects as go
import os
import sys
import pandas as pd
import plotly.express as px
from scipy import stats
import requests

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
        
    def _aggregate_data(self, data: List[Dict], aggregate: bool, data_type: str = 'yearly') -> List[Dict]:
        if not aggregate:
            return data
            
        aggregated_data = []
        easily_sums = {}
        doc_externe_sums = {}
        
        if data_type == 'yearly':
            for entry in data:
                origin = entry['document_origin_code']
                year = entry['year']
                count = entry['count']
                
                if origin.startswith('Easil'):
                    easily_sums[year] = easily_sums.get(year, 0) + count
                elif origin.startswith('DOC_EXTERN'):
                    doc_externe_sums[year] = doc_externe_sums.get(year, 0) + count
                else:
                    aggregated_data.append(entry)
            
            for year in sorted(easily_sums):
                aggregated_data.append({
                    'document_origin_code': 'Easily_ALL',
                    'year': year,
                    'count': easily_sums[year]
                })
                
            for year in sorted(doc_externe_sums):
                aggregated_data.append({
                    'document_origin_code': 'DOC_EXTERNE_ALL',
                    'year': year,
                    'count': doc_externe_sums[year]
                })
                
        else:
            df = pd.DataFrame(data)
            df['month'] = pd.to_datetime(df['month'])
            
            easily_mask = df['document_origin_code'].str.startswith('Easil')
            doc_externe_mask = df['document_origin_code'].str.startswith('DOC_EXTERN')
            
            aggregated_data = df[~(easily_mask | doc_externe_mask)].to_dict('records')
            
            if easily_mask.any():
                easily_agg = df[easily_mask].groupby('month')['count'].sum().reset_index()
                easily_agg['document_origin_code'] = 'Easily_ALL'
                aggregated_data.extend(easily_agg.to_dict('records'))
                
            if doc_externe_mask.any():
                doc_externe_agg = df[doc_externe_mask].groupby('month')['count'].sum().reset_index()
                doc_externe_agg['document_origin_code'] = 'DOC_EXTERNE_ALL'
                aggregated_data.extend(doc_externe_agg.to_dict('records'))
        
        return aggregated_data
    
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
        """Display document distribution section with enhanced statistics."""
        with st.expander("ℹ️ À propos de la distribution des documents"):
            st.markdown("""
            ### 📊 Vue d'Ensemble
            Cette section analyse la distribution des documents dans l'EDS selon différentes perspectives :
            
            **Sources de Données :**
            - **Historique Complet** : Ensemble des documents uniques sur toute la période
            - **Documents Récents** : Documents des 7 derniers jours
            
            **Métriques Clés :**
            - **Volume Total** : Nombre total de documents dans l'EDS
            - **Moyenne par Type** : Distribution moyenne entre les différentes catégories
            - **Type le Plus Actif** : Catégorie avec le plus grand volume de documents
            
            ### 📈 Interprétation des Graphiques
            
            **1. Distribution Globale :**
            - Montre la répartition en pourcentage de chaque type de document
            - Permet d'identifier les catégories dominantes
            
            **2. Analyse Pareto :**
            - Graphique à barres + ligne cumulative
            - Aide à identifier les types de documents qui représentent la majorité du volume
            - Suit le principe 80/20 (20% des types représentent souvent 80% du volume)
            
            **3. Corrélation Volume Total vs Récent :**
            - Compare le volume historique au volume récent
            - Points au-dessus de la diagonale : plus actifs récemment
            - Points en-dessous : moins actifs récemment
            
            **4. Comparaison et Tendances :**
            - Met en parallèle les volumes totaux et récents
            - Permet d'identifier les évolutions et changements de tendances
            
            ### 📉 Indicateurs de Performance
            
            **Ratio Récent/Total :**
            - < 0.5 : Activité en baisse
            - ≈ 1.0 : Activité stable
            - > 1.5 : Activité en hausse significative
            
            **Changements Relatifs :**
            - 📈 Croissance : Augmentation du volume récent
            - 📉 Décroissance : Diminution du volume récent
            """)

        st.header("📑 Distribution des Documents")
        
        doc_counts = self.fetch_data("document_counts", use_simulation)
        recent_doc_counts = self.fetch_data("recent_document_counts", use_simulation)

        if doc_counts and recent_doc_counts:
            # Convert to DataFrames
            df_all = pd.DataFrame(doc_counts)
            df_recent = pd.DataFrame(recent_doc_counts)

            # Merge the data
            df_merged = pd.merge(
                df_all.rename(columns={'unique_document_count': 'total_count'}),
                df_recent.rename(columns={'unique_document_count': 'recent_count'}),
                on='document_origin_code'
            )

            tab1, tab2 = st.tabs(["📊 Vue Générale", "📈 Analyses Détaillées"])

            with tab1:
                # Metrics Overview
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    total_docs = df_merged['total_count'].sum()
                    recent_docs = df_merged['recent_count'].sum()
                    st.metric(
                        "Volume Total",
                        f"{total_docs:,}",
                        f"+{recent_docs:,} derniers 7 jours"
                    )

                with col2:
                    avg_per_type = df_merged['total_count'].mean()
                    st.metric(
                        "Moyenne par Type",
                        f"{avg_per_type:,.0f}",
                        f"±{df_merged['total_count'].std():,.0f} (écart-type)"
                    )

                with col3:
                    most_active = df_merged.loc[df_merged['total_count'].idxmax()]
                    st.metric(
                        "Type le Plus Actif",
                        most_active['document_origin_code'],
                        f"{most_active['total_count']:,} documents"
                    )

                # Distribution Charts
                self.chart_display.create_document_distribution_chart(
                    doc_counts, "Distribution Globale des Documents"
                )

            with tab2:
                st.subheader("📊 Analyse Statistique Détaillée")
                st.info("""                
                **Le tableau ci-dessous montre :**
                - La répartition détaillée par type de document
                - Le pourcentage que représente chaque type
                - L'activité récente (7 derniers jours)
                - Le ratio entre l'activité récente et historique
                """)
                

                # Calculate additional statistics
                df_merged['proportion'] = df_merged['total_count'] / total_docs * 100
                df_merged['recent_ratio'] = df_merged['recent_count'] / df_merged['total_count']
                
                # Sort by total count
                df_stats = df_merged.sort_values('total_count', ascending=False)

                # Display detailed stats table
                st.write("##### 📋 Statistiques par Type de Document")
                stats_df = pd.DataFrame({
                    'Type': df_stats['document_origin_code'],
                    'Volume Total': df_stats['total_count'].apply(lambda x: f"{x:,}"),
                    'Part (%)': df_stats['proportion'].apply(lambda x: f"{x:.1f}%"),
                    '7 Derniers Jours': df_stats['recent_count'].apply(lambda x: f"{x:,}"),
                    'Ratio Récent/Total': df_stats['recent_ratio'].apply(lambda x: f"{x:.2f}")
                })
                st.dataframe(stats_df, use_container_width=True)

                # Activity Analysis
                st.write("##### 📈 Analyse de l'Activité")
                st.info("""            
            **Diagramme de Pareto :**
            - Les barres bleues montrent la proportion de chaque type
            - La ligne rouge montre le cumul des proportions
            - Aide à identifier les types de documents prioritaires
            
            **Graphique de Corrélation :**
            - Compare les volumes récents aux volumes totaux
            - Plus les points sont proches de la diagonale, plus l'activité est stable
            - Les points éloignés indiquent des changements d'activité
            """)
                col1, col2 = st.columns(2)

                with col1:
                    # Pareto Chart
                    cumsum = df_stats['proportion'].cumsum()
                    fig_pareto = go.Figure()
                    
                    fig_pareto.add_trace(go.Bar(
                        x=df_stats['document_origin_code'],
                        y=df_stats['proportion'],
                        name='Proportion'
                    ))
                    
                    fig_pareto.add_trace(go.Scatter(
                        x=df_stats['document_origin_code'],
                        y=cumsum,
                        name='Cumul',
                        line=dict(color='red'),
                        yaxis='y2'
                    ))
                    
                    fig_pareto.update_layout(
                        title="Analyse Pareto des Types de Documents",
                        yaxis=dict(title="Proportion (%)"),
                        yaxis2=dict(title="Cumul (%)", overlaying='y', side='right'),
                        showlegend=True
                    )
                    
                    st.plotly_chart(fig_pareto, use_container_width=True)

                with col2:
                    # Recent vs Total Scatter
                    fig_scatter = px.scatter(
                        df_merged,
                        x='total_count',
                        y='recent_count',
                        text='document_origin_code',
                        title="Corrélation Volume Total vs Récent"
                    )
                    
                    fig_scatter.update_traces(
                        textposition='top center',
                        marker=dict(size=10)
                    )
                    
                    st.plotly_chart(fig_scatter, use_container_width=True)


        else:
            st.warning("Aucune donnée disponible pour la distribution des documents.")

    def display_automated_insights(self, yearly_data: List[Dict], monthly_data: List[Dict]):
        """Affiche des insights automatisés basés sur l'analyse des données."""


        # Conversion en DataFrames
        df_yearly = pd.DataFrame(yearly_data)
        df_monthly = pd.DataFrame(monthly_data)
        print(df_monthly['month'])
        df_monthly['month'] = pd.to_datetime(df_monthly['month'])
        print(df_monthly['month'])
        
        # Analyse par connecteur
        for connector in df_yearly['document_origin_code'].unique():
            with st.expander(f"📊 Analyse - {connector}"):
                # Données annuelles du connecteur
                yearly_connector = df_yearly[df_yearly['document_origin_code'] == connector]
                
                # Calcul de la croissance annuelle
                yearly_counts = yearly_connector.set_index('year')['count']
                yearly_growth = yearly_counts.pct_change() * 100
                
                # Tendance générale
                if len(yearly_counts) >= 2:
                    overall_growth = ((yearly_counts.iloc[-1] / yearly_counts.iloc[0]) - 1) * 100
                    #st.write(f"##### Tendance Générale")
                    st.write(f"🔀 Evolution sur la période : {overall_growth:.1f}% "
                            f"({yearly_counts.iloc[0]:,} → {yearly_counts.iloc[-1]:,} documents)")
                    
                    # Analyse de la croissance
                    avg_growth = yearly_growth.mean()
                    growth_color = "🟢" if avg_growth > 0 else "🔴"
                    st.write(f"{growth_color} Croissance moyenne annuelle : {avg_growth:.1f}%")

                # Données mensuelles du connecteur
                monthly_connector = df_monthly[df_monthly['document_origin_code'] == connector]
                
                if not monthly_connector.empty:
                    #st.write("##### Analyse Mensuelle")
                    
                    # Calcul des statistiques mensuelles
                    current_month = monthly_connector.iloc[-1]
                    prev_month = monthly_connector.iloc[-2] if len(monthly_connector) > 1 else None
                    
                    if prev_month is not None:
                        month_growth = ((current_month['count'] / prev_month['count']) - 1) * 100
                        growth_icon = "📈" if month_growth > 0 else "📉"
                        st.write(f"{growth_icon} Croissance sur le dernier mois : {month_growth:.1f}%  "
                                f"({prev_month['count']:,} → {current_month['count']:,})")

                    # Détection des anomalies
                    mean = monthly_connector['count'].mean()
                    std = monthly_connector['count'].std()
                    last_value = current_month['count']
                    
                    if abs(last_value - mean) > 2 * std:
                        if last_value > mean:
                            st.warning(f"⚠️ Volume inhabituellement élevé le dernier mois "
                                    f"({last_value:,} vs moyenne de {mean:.0f})")
                        else:
                            st.warning(f"⚠️ Volume inhabituellement bas le dernier mois "
                                    f"({last_value:,} vs moyenne de {mean:.0f})")

                    # Identification des mois exceptionnels
                    peak_month = monthly_connector.loc[monthly_connector['count'].idxmax()]
                    low_month = monthly_connector.loc[monthly_connector['count'].idxmin()]
                    
                    #st.write("##### Points Remarquables")
                    st.write(f"🔥 Pic d'activité : {peak_month['month'].strftime('%B %Y')} "
                            f"avec {peak_month['count']:,} documents")
                    st.write(f"⬇️ Plus faible activité : {low_month['month'].strftime('%B %Y')} "
                            f"avec {low_month['count']:,} documents")



        # Dynamique récente
        recent_growth = df_monthly.groupby('document_origin_code').agg({
            'count': ['mean', 'std']
        })
        
        st.write("##### Stabilité des Connecteurs")
        st.info("""
        La stabilité est évaluée grâce au Coefficient de Variation (CV) qui mesure la dispersion relative des données :
        - 🟢 **Stable** (CV ≤ 20%) : Faible variabilité, flux de documents régulier
        - 🟡 **Variable** (20% < CV < 40%) : Variabilité modérée, possibles variations saisonnières
        - 🔴 **Très variable** (CV ≥ 40%) : Forte variabilité, possibles anomalies à investiguer
        
        *CV = (Écart-type / Moyenne) × 100*
        """)
        for connector in recent_growth.index:
            mean = recent_growth.loc[connector, ('count', 'mean')]
            std = recent_growth.loc[connector, ('count', 'std')]
            cv = (std / mean) * 100  # Coefficient de variation
            
            stability = "🟢" if cv <= 20 else "🟡" if  20 < cv < 40 else "🔴"
            st.write(f"{stability} {connector}: "
                    f"{'Stable' if cv < 20 else 'Variable' if cv < 40 else 'Très variable'} "
                    f"(CV: {cv:.1f}%)")
            
    def display_connector_monitoring(self, use_simulation: bool):
        """Display connector monitoring section."""
        try:
            st.header("📈 Monitoring des connecteurs")

            origin_codes = [
                "ARCH_INTERNE", "BIO", "CYBERLAB",
                "DOC_EXTERNE_Ari", "DOC_EXTERNE_Api", "DOC_EXTERNE_CeS",
                "DOC_EXTERNE_Car", "DOC_EXTERNE_COP", "DOC_EXTERNE_DIA",
                "DOC_EXTERNE_ECG", "DOC_EXTERNE_None", "DOC_EXTERNE_Pat",
                "DOC_EXTERNE_Res", "DOC_EXTERNE_SOF", "DOC_EXTERNE_SPI",
                "DOC_EXTERNE_XPl", "DOC_EXTERNE_vie", "DOC_EXTERNE_Med",
                "DOC_EXTERNE_PCA", "Easily", "Easily_Ari", "Easily_Car",
                "Easily_CeS", "Easily_COP", "Easily_DIA", "Easily_echo_cardio",
                "Easily_Efitback", "Easily_EFR", "Easily_Med", "Easily_Muse",
                "Easily_Patientys", "Easily_PCA", "Easily_Res", "Easily_SOF",
                "Easily_Xpl", "FOCH_EFR", "RDV_DOCTOLIB"
            ]

            tab1, tab2, tab3 = st.tabs(["📈 Graphiques", "📊 Statistiques", "ℹ️ Analyse"])
            
            with tab1:
                st.info("""
                📊 Visualisation des séries temporelles
                
                Ce graphique montre l'évolution du nombre de documents par connecteur au fil du temps.
                
                - Survolez les points pour voir les détails
                - Double-cliquez sur une légende pour isoler un connecteur
                - Cliquez sur les boutons de zoom en haut pour ajuster la vue
                - Utilisez la souris pour zoomer sur une période spécifique
                
                Option d'agrégation:
                - Activez le bouton pour regrouper les connecteurs Easily et DOC_EXTERNE
                """)
                
                # Add toggle for aggregation
                aggregate_data = st.toggle('Regrouper les connecteurs Easily et DOC_EXTERNE', value=False)
                
                if aggregate_data:
                    processed_origins = self._aggregate_origin_codes(origin_codes)
                else:
                    processed_origins = origin_codes
                    
                self.display_time_series_data(processed_origins, use_simulation, aggregate_data)
                
            with tab2:
                params = {"origin_codes": ",".join(processed_origins)}
                yearly_data = self.fetch_data("document_counts_by_year", use_simulation, params=params)
                monthly_data = self.fetch_data("recent_document_counts_by_month", use_simulation, params=params)
                
                if yearly_data and monthly_data:
                    self.metrics_display.display_connector_statistics(yearly_data, monthly_data)
                else:
                    st.warning("Données insuffisantes pour calculer les statistiques.")
                    
            with tab3:
                st.write("#### 🔍 Analyse des Tendances")
                if yearly_data and monthly_data:
                    self.display_automated_insights(yearly_data, monthly_data)

        except Exception as e:
            print("\n=== Debug: Error in display_connector_monitoring ===")
            print(f"Error type: {type(e).__name__}")
            print(f"Error message: {str(e)}")
            st.error(f"Une erreur s'est produite : {str(e)}")
            logger.error(f"Error in display_connector_monitoring: {str(e)}")

    def _aggregate_origin_codes(self, origin_codes: List[str]) -> List[str]:
        """Aggregate origin codes for Easily and DOC_EXTERNE."""
        aggregated_codes = []
        easily_codes = []
        doc_externe_codes = []
        
        for code in origin_codes:
            if code.startswith('Easil'):
                easily_codes.append(code)
            elif code.startswith('DOC_EXTERN'):
                doc_externe_codes.append(code)
            else:
                aggregated_codes.append(code)
        
        if easily_codes:
            aggregated_codes.append('Easily_ALL')
        if doc_externe_codes:
            aggregated_codes.append('DOC_EXTERNE_ALL')
        
        return aggregated_codes
    


    def _get_api_data(self, endpoint_key: str, params: Optional[Dict] = None) -> Optional[Dict]:
        if endpoint_key not in self.api_endpoints:
            raise ValueError(f"Unknown endpoint key: {endpoint_key}")

        try:
            aggregate = params.pop('aggregate', 'false') == 'true' if params else False
            data_type = 'yearly' if 'year' in endpoint_key else 'monthly'

            if endpoint_key in st.session_state.endpoint_cache:
                data = st.session_state.endpoint_cache[endpoint_key]
                if isinstance(data, list):
                    return self._aggregate_data(data, aggregate, data_type)
                return data

            url = f"{self.base_url}{self.api_endpoints[endpoint_key]}"
            response = requests.get(url, params=params)
            
            if response.status_code == 400:
                error_detail = response.json().get("detail", "Bad request")
                raise ValueError(f"API Error: {error_detail}")
                
            response.raise_for_status()
            data = response.json()

            st.session_state.endpoint_cache[endpoint_key] = data

            if isinstance(data, list):
                return self._aggregate_data(data, aggregate, data_type)
            return data
            
        except requests.RequestException as e:
            logger.error(f"API request failed: {str(e)}")
            raise

    def display_time_series_data(self, selected_origins: List[str], use_simulation: bool, aggregate_data: bool = False):
        """Display time series data for selected origins."""
        try:
            origin_codes_str = ",".join(selected_origins)
            params = {"origin_codes": origin_codes_str}
            
            with st.spinner("Chargement des données..."):
                yearly_data = self.fetch_data("document_counts_by_year", use_simulation, params=params)
                monthly_data = self.fetch_data("recent_document_counts_by_month", use_simulation, params=params)
                
                if aggregate_data:
                    yearly_data = self._aggregate_data(yearly_data, True, 'yearly')
                    monthly_data = self._aggregate_data(monthly_data, True, 'monthly')

            if yearly_data and monthly_data:
                tab1, tab2 = st.tabs(["Tendance Annuelle", "Tendance Mensuelle"])

                with tab1:
                    if yearly_data:
                        self.chart_display.create_time_series_chart(
                            yearly_data,
                            "year",
                            "Nombre de Documents par Année",
                            show_range_selector=False
                        )
                    else:
                        st.info("Aucune donnée annuelle disponible.")

                with tab2:
                    if monthly_data:
                        self.chart_display.create_time_series_chart(
                            monthly_data,
                            "month",
                            "Nombre de Documents Récents par Mois",
                            show_range_selector=False
                        )
                    else:
                        st.info("Aucune donnée mensuelle disponible.")
            else:
                st.warning("Aucune donnée disponible pour les origines sélectionnées.")

        except Exception as e:
            print("\n=== Debug: Error in display_time_series_data ===")
            print(f"Error type: {type(e).__name__}")
            print(f"Error message: {str(e)}")
            st.error(f"Erreur lors de la récupération des données: {str(e)}")
            logger.error(f"Error in display_time_series_data: {str(e)}")

    def display_user_activity(self, use_simulation: bool):
        """Display user activity section with detailed statistics."""
        st.header("👥 Activité Utilisateurs")
        # Update info expandable
        with st.expander("ℹ️ À propos de l'Activité Utilisateurs"):
            st.markdown("""
            **Analyse des Requêtes Utilisateurs:**
            - Les 10 premiers utilisateurs par nombre de requêtes
            - Les membres de l'équipe CODOC sont regroupés sous 'CODOC'
            - Identification des utilisateurs peu actifs (≤3 requêtes)
            - Statistiques globales d'utilisation
            - Les comptages sont basés sur la table DWH_LOG_QUERY
            """)
        # Fetch all user data
        top_users = self.fetch_data("top_users", use_simulation)
        current_year_users = self.fetch_data("top_users_current_year", use_simulation)
        user_stats = self.fetch_data("users_stats", use_simulation)
        
        # Overview metrics
        if user_stats:
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric(
                    "Utilisateurs Actifs", 
                    f"{user_stats['total_users']}",
                    help="Nombre total d'utilisateurs ayant effectué au moins une requête (année en cours)"
                )
            with col2:
                st.metric(
                    "Utilisateurs Peu Actifs (≤3 requêtes)", 
                    f"{user_stats['low_activity_users_count']} ({user_stats['low_activity_percentage']:.1f}%)",
                    help="Utilisateurs ayant fait 3 requêtes ou moins"
                )
            with col3:
                st.metric(
                    "Moyenne de Requêtes", 
                    f"{user_stats['avg_queries']:.1f}",
                    help="Nombre moyen de requêtes par utilisateur"
                )
        
        # Activity charts in tabs
        tab1, tab2, tab3 = st.tabs(["📊 Historique Complet", "📈 Année en Cours", "🔍 Utilisateurs Peu Actifs"])
        
        with tab3:
            if user_stats and user_stats['low_activity_details']:
                st.info("Liste des utilisateurs ayant effectué 3 requêtes ou moins")
                
                # Create DataFrame for low activity users
                df_low = pd.DataFrame(user_stats['low_activity_details'])
                df_low['name'] = df_low['name'].apply(lambda x: ' '.join(x.split()))

                # Display as bar chart
                fig = px.bar(df_low, 
                            x='name', 
                            y='count',
                            title="Utilisateurs Peu Actifs (≤3 requêtes)",
                            labels={'name': 'Utilisateur', 'count': 'Nombre de requêtes'})
                
                fig.update_layout(showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
                
                # Create DataFrame
                df = pd.DataFrame(user_stats['low_activity_details'])
                df.columns = ['Utilisateur', 'Nombre de requêtes']
                df['Utilisateur'] = df['Utilisateur'].apply(lambda x: ' '.join(x.split()))

                # Display as Streamlit dataframe with custom styling
                st.dataframe(
                df,
                column_config={
                    "Utilisateur": "Utilisateur",
                    "Nombre de requêtes": st.column_config.NumberColumn(
                        "Nombre de requêtes",
                        help="Nombre de requêtes effectuées",
                        format="%d"
                    )
                },
                hide_index=True,
                use_container_width=True
                )
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
            - Calculée depuis la date de création du document (DOCUMENT_DATE) du plus ancien document
            - Les documents de plus de 20 ans sont candidats à la suppression
            """)

        archive_data = self.fetch_data("archive_status", use_simulation)
        if archive_data:
            self.metrics_display.display_archive_metrics(archive_data)
            self.chart_display.create_archive_chart(archive_data)

    def display_pmsi(self, use_simulation: bool):
        """Display PMSI analytics section."""
        st.header("📋 Documents PMSI")
        
        pmsi_data = self.fetch_data("pmsi", use_simulation)
        if not pmsi_data:
            st.warning("Aucune donnée PMSI disponible.")
            return
            
        # Overview metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric(
                "Dernier chargement",
                pmsi_data["last_upload"],
                help="Date et heure du dernier document PMSI chargé"
            )
        with col2:
            st.metric(
                "Période couverte",
                f"{pmsi_data['time_period']['months_count']} mois",
                help="Nombre de mois entre le premier et le dernier document"
            )
        with col3:
            st.metric(
                "Mois avec données manquantes",
                len(pmsi_data["gaps"]),
                help="Nombre de mois sans données dans la période"
            )
            
        # Monthly trend visualization
        if pmsi_data["monthly_counts"]:
            df = pd.DataFrame(pmsi_data["monthly_counts"])
            df['month'] = pd.to_datetime(df['month'])
            
            fig = px.line(
                df,
                x='month',
                y='count',
                title="Evolution mensuelle des documents PMSI",
                labels={'count': 'Nombre de documents', 'month': 'Mois'}
            )
            
            # Add gaps visualization
            if pmsi_data["gaps"]:
                gap_dates = pd.to_datetime(pmsi_data["gaps"])
                fig.add_scatter(
                    x=gap_dates,
                    y=[0] * len(gap_dates),
                    mode='markers',
                    marker=dict(color='red', size=10, symbol='x'),
                    name='Mois manquants'
                )
                
            fig.update_layout(
                hovermode='x unified',
                showlegend=True
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Statistics
            st.subheader("📊 Statistiques")
            stats_cols = st.columns(4)
            with stats_cols[0]:
                st.metric(
                    "Total documents",
                    f"{pmsi_data['stats']['total_documents']:,}",
                    help="Nombre total de documents PMSI"
                )
            with stats_cols[1]:
                st.metric(
                    "Moyenne mensuelle",
                    f"{pmsi_data['stats']['avg_monthly_documents']:.0f}",
                    help="Moyenne de documents par mois"
                )
            with stats_cols[2]:
                st.metric(
                    "Maximum mensuel",
                    f"{pmsi_data['stats']['max_monthly_documents']:,}",
                    help="Plus grand nombre de documents sur un mois"
                )
            with stats_cols[3]:
                st.metric(
                    "Gaps",
                    f"{pmsi_data['stats']['months_with_gaps']} mois",
                    help="Nombre de mois sans données"
                )

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
                ✦ Documents éligibles à la suppression<br>
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
        st.caption("Vue d'ensemble des indicateurs de la base de données")

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
