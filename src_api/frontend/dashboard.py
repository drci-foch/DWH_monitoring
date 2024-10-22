import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import logging

from fetch_function import fetch_data, fetch_simulated_data

logging.basicConfig(level=logging.INFO)

# Layout Setup
def create_enhanced_layout():
    st.set_page_config(
        page_title="Monitoring de l'Entrepôt de Donnée de Santé - Base",
        layout="wide",
        initial_sidebar_state="expanded"
    )

# Main Dashboard
def main():
    create_enhanced_layout()
    
    st.title("Monitoring de l'Entrepôt de Donnée de Santé")
    st.caption("Vue d'ensemble complète des indicateurs de la base de données : métriques de qualité, activité des utilisateurs et répartition des documents")
    
    # Sidebar controls
    with st.sidebar:
        st.title("Contrôles du Dashboard")
        st.divider()
        
        use_simulation = st.toggle("Utiliser des données simulées", value=True, 
                                    help="Basculer entre les données simulées et réelles")
                
        if st.button("🔄 Actualiser les données", type="primary", use_container_width=True):
            st.rerun()
                
        with st.expander("À propos"):
            st.write("""Ce tableau de bord fournit une analyse en temps réel de la base de données, 
                    comprenant les indicateurs de qualité, les tendances d'utilisation, 
                    et la répartition des documents.
                    """)
    
# Section résumé
    st.header("📊 Métriques Générales")
    with st.expander("ℹ️ À propos de ces métriques"):
        st.markdown("""
        - **Patients Totaux**: Nombre de patients uniques dans la base de données
        - **Patients Test**: Patients avec le nom de famille 'TEST'
        - **Patients Recherche**: Patients avec le nom de famille 'FLEUR'
        - **Patients Sensibles**: Patients avec le nom de famille 'INSECTE'
        - Tous les décomptes sont basés sur les numéros uniques de patients (PATIENT_NUM)
        """)
    summary = fetch_simulated_data("/summary/api/summary") if use_simulation else fetch_data("/summary/api/summary")
    
    if summary:
        cols = st.columns(6)
        metrics = [
            ("👥 Total Patients", summary["patient_count"]),
            ("🧪 Patients Test", summary["test_patient_count"]),
            ("🔬 Patients Recherche", summary["research_patient_count"]),
            ("⭐ Patients Sensibles", summary["celebrity_patient_count"]),
            ("📄 Documents Totaux", summary["total_documents"]),
            ("📥 Documents Récents", summary["recent_documents"])
        ]
        
        for col, (label, value) in zip(cols, metrics):
            with col:
                st.metric(label, f"{value:,}")
                
# Document Distribution section
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
    # Get document counts
    if use_simulation:
        doc_counts = fetch_simulated_data("/api/document_counts")
        recent_doc_counts = fetch_simulated_data("/api/recent_document_counts")
    else:
        doc_counts = fetch_data("/api/document_counts")
        recent_doc_counts = fetch_data("/api/recent_document_counts")
    
    if doc_counts or recent_doc_counts:
        tab1, tab2 = st.tabs(["Historique Complet", "Documents Récents"])
        
        with tab1:
            if doc_counts:
                df_doc_counts = pd.DataFrame(doc_counts)
                fig = px.pie(df_doc_counts,
                            values="unique_document_count",
                            names="document_origin_code",
                            title="Distribution des Documents par Origine",
                            hole=0.4)
                fig.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig, use_container_width=True, key="doc_distribution_all")
                
                total_docs = df_doc_counts['unique_document_count'].sum()
                st.metric("Documents Totaux", f"{total_docs:,}")
            else:
                st.warning("Les données de documents ne sont pas disponibles.")
        
        with tab2:
            if recent_doc_counts:
                df_recent_doc_counts = pd.DataFrame(recent_doc_counts)
                fig = px.pie(df_recent_doc_counts,
                            values="unique_document_count",
                            names="document_origin_code",
                            title="Distribution des Documents Récents par Origine",
                            hole=0.4)
                fig.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig, use_container_width=True, key="doc_distribution_recent")
                
                total_recent_docs = df_recent_doc_counts['unique_document_count'].sum()
                st.metric("Documents Récents Totaux", f"{total_recent_docs:,}")
            else:
                st.warning("Les données des documents récents ne sont pas disponibles.")
                
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np
import random
import math
import json
import requests
from typing import Dict, List, Any, Optional
import logging

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ... (Fonctions de génération de données et fetch_data restent identiques)

def main():
    create_enhanced_layout()
    
    st.title("Tableau de Bord de Qualité des Données")
    st.caption("Vue d'ensemble de la qualité de la base de données : métriques, activité utilisateurs et distribution des documents")
    
    # Contrôles de la barre latérale
    with st.sidebar:
        st.title("Contrôles du Tableau de Bord")
        st.divider()
        
        use_simulation = st.toggle("Utiliser des données simulées", value=True, 
                               help="Basculer entre les données simulées et réelles")
        
        if st.button("🔄 Actualiser les données", type="primary", use_container_width=True):
            st.rerun()
        
        with st.expander("À propos"):
            st.write("""
            Ce tableau de bord fournit une analyse en temps réel de la base de données, 
            comprenant les indicateurs de qualité, les tendances d'utilisation, 
            et la répartition des documents.
            """)
    
    # Section résumé
    st.header("📊 Métriques Générales")
    with st.expander("ℹ️ À propos de ces métriques"):
        st.markdown("""
        - **Patients Totaux**: Nombre de patients uniques dans la base de données
        - **Patients Test**: Patients avec le nom de famille 'TEST'
        - **Patients Recherche**: Patients avec le nom de famille 'INSECTE'
        - **Patients Sensibles**: Patients avec le nom de famille 'FLEUR'
        - Tous les décomptes sont basés sur les numéros uniques de patients (PATIENT_NUM)
        """)
    summary = fetch_simulated_data("/summary/api/summary") if use_simulation else fetch_data("/summary/api/summary")
    
    if summary:
        cols = st.columns(6)
        metrics = [
            ("👥 Total Patients", summary["patient_count"]),
            ("🧪 Patients Test", summary["test_patient_count"]),
            ("🔬 Patients Recherche", summary["research_patient_count"]),
            ("⭐ Patients Sensibles", summary["celebrity_patient_count"]),
            ("📄 Documents Totaux", summary["total_documents"]),
            ("📥 Documents Récents", summary["recent_documents"])
        ]
        
        for col, (label, value) in zip(cols, metrics):
            with col:
                st.metric(label, f"{value:,}")
    
    # Section Distribution des Documents
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

    # Obtention des comptages de documents
    if use_simulation:
        doc_counts = fetch_simulated_data("/api/document_counts")
        recent_doc_counts = fetch_simulated_data("/api/recent_document_counts")
    else:
        doc_counts = fetch_data("/api/document_counts")
        recent_doc_counts = fetch_data("/api/recent_document_counts")
    
    if doc_counts or recent_doc_counts:
        tab1, tab2 = st.tabs(["Historique Complet", "Documents Récents"])
        
        with tab1:
            if doc_counts:
                df_doc_counts = pd.DataFrame(doc_counts)
                fig = px.pie(df_doc_counts,
                            values="unique_document_count",
                            names="document_origin_code",
                            title="Distribution des Documents par Origine",
                            hole=0.4)
                fig.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig, use_container_width=True, key="doc_distribution_all")
                
                total_docs = df_doc_counts['unique_document_count'].sum()
                st.metric("Documents Totaux", f"{total_docs:,}")
            else:
                st.warning("Les données de documents ne sont pas disponibles.")
        
        with tab2:
            if recent_doc_counts:
                df_recent_doc_counts = pd.DataFrame(recent_doc_counts)
                fig = px.pie(df_recent_doc_counts,
                            values="unique_document_count",
                            names="document_origin_code",
                            title="Distribution des Documents Récents par Origine",
                            hole=0.4)
                fig.update_traces(textposition='inside', textinfo='percent+label')
                st.plotly_chart(fig, use_container_width=True, key="doc_distribution_recent")
                
                total_recent_docs = df_recent_doc_counts['unique_document_count'].sum()
                st.metric("Documents Récents Totaux", f"{total_recent_docs:,}")
            else:
                st.warning("Les données des documents récents ne sont pas disponibles.")
    
    # Section Monitoring des connecteurs
    st.header("📈 Monitoring des connecteurs")
    with st.expander("ℹ️ À propos du Monitoring des connecteurs'"):
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
        
        **Note:** Les décomptes de documents sont toujours basés sur des identifiants uniques pour garantir une représentation précise.
        """)

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
        
        if not selected_origins:
            st.warning("Veuillez sélectionner au moins une origine de document à afficher.")
        else:
            if use_simulation:
                doc_counts_by_year = fetch_simulated_data(
                    "/sources/document_counts_by_year",
                    params={"origin_codes": selected_origins}
                )
                recent_doc_counts_by_month = fetch_simulated_data(
                    "api/v1/sources/recent_document_counts_by_month",
                    params={"origin_codes": selected_origins}
                )
            else:
                doc_counts_by_year = fetch_data(
                    "/sources/document_counts_by_year",
                    params={"origin_codes": selected_origins}
                )
                recent_doc_counts_by_month = fetch_data(
                    "api/v1/sources/recent_document_counts_by_month",
                    params={"origin_codes": selected_origins}
                )
            
            if doc_counts_by_year and recent_doc_counts_by_month:
                df_yearly = pd.DataFrame(doc_counts_by_year)
                df_yearly['year'] = pd.to_datetime(df_yearly['year'].astype(str))
                
                df_monthly = pd.DataFrame(recent_doc_counts_by_month)
                df_monthly['month'] = pd.to_datetime(df_monthly['month'])
                
                tab1, tab2 = st.tabs(["Tendance Annuelle", "Tendance Mensuelle"])
                
                with tab1:
                    total_yearly = df_yearly.groupby('year')['count'].sum().mean()
                    st.metric("Moyenne Annuelle de Documents", f"{total_yearly:,.0f}")
                    
                    fig = px.line(df_yearly,
                                x="year",
                                y="count",
                                color="document_origin_code",
                                title="Nombre de Documents par Année")
                    
                    fig.update_layout(
                        xaxis_title="Année",
                        yaxis_title="Nombre de Documents",
                        legend_title="Origine du Document",
                        hovermode='x unified'
                    )
                    st.plotly_chart(fig, use_container_width=True, key="yearly_trend")
                
                with tab2:
                    total_monthly = df_monthly.groupby('month')['count'].sum().mean()
                    st.metric("Moyenne Mensuelle de Documents", f"{total_monthly:,.0f}")
                    
                    fig = px.line(df_monthly,
                                x="month",
                                y="count",
                                color="document_origin_code",
                                title="Nombre de Documents Récents par Mois")
                    
                    fig.update_layout(
                        xaxis_title="Mois",
                        yaxis_title="Nombre de Documents",
                        legend_title="Origine du Document",
                        hovermode='x unified'
                    )
                    
                    fig.update_xaxes(
                        rangeslider_visible=True,
                        rangeselector=dict(
                            buttons=list([
                                dict(count=3, label="3m", step="month", stepmode="backward"),
                                dict(count=6, label="6m", step="month", stepmode="backward"),
                                dict(count=1, label="1a", step="year", stepmode="backward"),
                                dict(step="all", label="Tout")
                            ])
                        )
                    )
                    st.plotly_chart(fig, use_container_width=True, key="monthly_trend")
                
                col1, col2 = st.columns(2)
                with col1:
                    st.download_button(
                        label="Télécharger les Données Annuelles",
                        data=df_yearly.to_csv(index=False),
                        file_name="comptage_documents_annuel.csv",
                        mime="text/csv"
                    )
                with col2:
                    st.download_button(
                        label="Télécharger les Données Mensuelles",
                        data=df_monthly.to_csv(index=False),
                        file_name="comptage_documents_mensuel.csv",
                        mime="text/csv"
                    )


    # User Activity section
    st.header("👥 Activité Utilisateurs")
    with st.expander("ℹ️ À propos de l'Activité Utilisateurs"):
        st.markdown("""
        **Analyse des Requêtes Utilisateurs :**
        - Affiche les 10 premiers utilisateurs par nombre de requêtes
        - Les membres de l'équipe CODOC sont regroupés sous 'CODOC'
        - Les comptages sont basés sur la table DWH_LOG_QUERY
        
        **Périodes :**
        - **Historique Complet** : Affiche les requêtes sur toute la période
        - **Année en Cours** : Affiche uniquement les requêtes de l'année en cours
        
        **Note :** Certains utilisateurs (admin, demo, support) sont regroupés pour une meilleure analyse.
        L'équipe CODOC inclut : admin admin, admin2 admin2, Demo Nicolas, ADMIN_ANONYM, etc.
        """)

    # Fetch user data
    if use_simulation:
        top_users_all = fetch_simulated_data("/api/top_users")
        top_users_year = fetch_simulated_data("/api/top_users_current_year")
    else:
        top_users_all = fetch_data("/api/top_users")
        top_users_year = fetch_data("/api/top_users_current_year")

    if top_users_all or top_users_year:
        tab1, tab2 = st.tabs(["Historique Complet", "Année en Cours"])
        
        with tab1:
            if top_users_all:
                df_users_all = pd.DataFrame(top_users_all)
                fig = px.bar(
                    df_users_all,
                    x="lastname",
                    y="query_count",
                    color="query_count",
                    text="query_count",
                    title="Top Utilisateurs par Nombre de Requêtes (Historique)",
                    labels={
                        "lastname": "Utilisateur",
                        "query_count": "Nombre de Requêtes",
                        "firstname": "Prénom"
                    },
                    hover_data=["firstname"],
                    color_continuous_scale="Viridis"
                )
                
                fig.update_traces(
                    textposition='outside',
                    texttemplate='%{text:,.0f}'
                )
                
                fig.update_layout(
                    showlegend=False,
                    hovermode='x unified',
                    hoverlabel=dict(bgcolor="white"),
                    margin=dict(t=30, l=60, r=20, b=60)
                )
                
                st.plotly_chart(fig, use_container_width=True, key="users_all_time")
                
                # Add summary metrics
                total_queries = df_users_all['query_count'].sum()
                avg_queries = df_users_all['query_count'].mean()
                
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Total des Requêtes", f"{total_queries:,.0f}")
                with col2:
                    st.metric("Moyenne des Requêtes par Utilisateur", f"{avg_queries:,.0f}")
                
                # Add detailed table
                st.subheader("Analyse Détaillée des Utilisateurs")
                detailed_df = df_users_all.copy()
                detailed_df['query_count'] = detailed_df['query_count'].apply(lambda x: f"{x:,}")
                st.dataframe(
                    detailed_df,
                    column_config={
                        "firstname": "Prénom",
                        "lastname": "Nom",
                        "query_count": "Nombre de Requêtes"
                    },
                    hide_index=True,
                    use_container_width=True
                )
            else:
                st.warning("Les données historiques des utilisateurs ne sont pas disponibles.")
        
        with tab2:
            if top_users_year:
                df_users_year = pd.DataFrame(top_users_year)
                fig = px.bar(
                    df_users_year,
                    x="lastname",
                    y="query_count",
                    color="query_count",
                    text="query_count",
                    title=f"Top Utilisateurs par Nombre de Requêtes ({datetime.now().year})",
                    labels={
                        "lastname": "Utilisateur",
                        "query_count": "Nombre de Requêtes",
                        "firstname": "Prénom"
                    },
                    hover_data=["firstname"],
                    color_continuous_scale="Viridis"
                )
                
                fig.update_traces(
                    textposition='outside',
                    texttemplate='%{text:,.0f}'
                )
                
                fig.update_layout(
                    showlegend=False,
                    hovermode='x unified',
                    hoverlabel=dict(bgcolor="white"),
                    margin=dict(t=30, l=60, r=20, b=60)
                )
                
                st.plotly_chart(fig, use_container_width=True, key="users_current_year")
                
                # Add summary metrics
                total_queries_year = df_users_year['query_count'].sum()
                avg_queries_year = df_users_year['query_count'].mean()
                
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Total des Requêtes (Cette Année)", f"{total_queries_year:,.0f}")
                with col2:
                    st.metric("Moyenne des Requêtes par Utilisateur (Cette Année)", f"{avg_queries_year:,.0f}")
                
                # Add download capability
                st.download_button(
                    label="📥 Télécharger les Données d'Activité",
                    data=df_users_year.to_csv(index=False),
                    file_name=f"activite_utilisateurs_{datetime.now().year}.csv",
                    mime="text/csv",
                    help="Télécharger les données complètes d'activité utilisateurs en CSV"
                )
            else:
                st.warning("Les données de l'année en cours ne sont pas disponibles.")
    else:
        st.warning("Les données d'activité utilisateurs ne sont pas disponibles.")
        # Archive Status section
    st.header("🗄️ Statut d'Archivage")
    with st.expander("ℹ️ À propos du Statut d'Archivage"):
        st.markdown("""
        **Période d'Archive :**
        - Calculée depuis la date de mise à jour (UPDATE_DATE) du plus ancien document jusqu'à aujourd'hui
        - Affichée en années pour plus de lisibilité
        
        **Critères de Suppression :**
        - Les documents sont marqués pour suppression lorsqu'ils ont plus de 20 ans (240 mois)
        - L'âge est calculé sur la base de UPDATE_DATE
        - `UPDATE_DATE < ADD_MONTHS(SYSDATE, -240)`
        
        **Documents à Supprimer :**
        - Regroupés par code d'origine du document
        - Triés par nombre de documents à supprimer
        - Affiche le nombre total et la distribution en pourcentage
        
        **Important :** Le seuil de 20 ans est un paramètre système fixe. Les documents plus anciens que ce seuil sont candidats à l'archivage/suppression.
        """)
    archive_status = fetch_simulated_data("/archives/api/archive_status") if use_simulation else fetch_data("/archives/api/archive_status")
    
    if archive_status:
        # Archive Period Information
        st.subheader("Analyse de la Période d'Archive")
        period_cols = st.columns([2, 1, 1])
        
        # Calculate dates for display
        archive_period = archive_status["archive_period"]
        current_date = datetime.now()
        oldest_date = current_date - timedelta(days=int(archive_period * 365.25))
        
        with period_cols[0]:
            st.metric(
                "Période d'Archive (années)", 
                f"{archive_period:.1f}",
                help="Durée entre le document le plus ancien et la date actuelle"
            )
        
        with period_cols[1]:
            st.metric(
                "Date du Document le Plus Ancien", 
                oldest_date.strftime("%Y-%m-%d"),
                help="Date du document le plus ancien dans le système"
            )
        
        with period_cols[2]:
            st.metric(
                "Seuil de Suppression",
                "20 ans",
                help="Les documents de plus de 20 ans (240 mois) sont marqués pour suppression"
            )
        
        # Suppression Information
        st.subheader("Documents à Supprimer (Plus de 20 ans)")
        
        suppress_cols = st.columns([2, 1])
        with suppress_cols[0]:
            st.metric(
                "Total des Documents à Supprimer", 
                f"{archive_status['total_documents_to_suppress']:,}",
                help="Nombre total de documents de plus de 20 ans"
            )
        
        if archive_status.get('documents_to_suppress'):
            # Convert tuple data to DataFrame
            df_suppress = pd.DataFrame(
                archive_status['documents_to_suppress'],
                columns=['Origine', 'Nombre']
            )
            
            # Calculate percentages
            total_to_suppress = df_suppress['Nombre'].sum()
            df_suppress['Pourcentage'] = (df_suppress['Nombre'] / total_to_suppress * 100).round(1)
            
            # Create visualization
            fig = go.Figure()
            
            # Add bar chart
            fig.add_trace(go.Bar(
                x=df_suppress['Origine'],
                y=df_suppress['Nombre'],
                text=df_suppress.apply(lambda x: f"{x['Nombre']:,}<br>{x['Pourcentage']:.1f}%", axis=1),
                textposition='outside',
                marker_color='#FF4B4B',
                name='Documents à Supprimer'
            ))
            
            # Update layout
            fig.update_layout(
                title="Documents à Supprimer par Origine (>20 ans)",
                xaxis_title="Origine du Document",
                yaxis_title="Nombre de Documents",
                showlegend=False,
                hovermode='x unified',
                hoverlabel=dict(
                    bgcolor="white",
                    font_size=12,
                ),
                margin=dict(t=30, l=60, r=20, b=60)
            )
            
            # Add custom hover template
            fig.update_traces(
                hovertemplate="<b>%{x}</b><br>" +
                             "Documents : %{y:,}<br>" +
                             "Pourcentage : %{text}<extra></extra>"
            )
            
            st.plotly_chart(fig, use_container_width=True, key="suppress_chart")
            
            # Detailed Analysis Table
            st.subheader("Analyse Détaillée des Suppressions")
            
            # Prepare table data
            table_data = df_suppress.copy()
            table_data['Nombre'] = table_data['Nombre'].apply(lambda x: f"{x:,}")
            table_data['Pourcentage'] = table_data['Pourcentage'].apply(lambda x: f"{x:.1f}%")
            
            # Display table
            st.dataframe(
                table_data,
                column_config={
                    "Origine": "Origine du Document",
                    "Nombre": "Documents à Supprimer",
                    "Pourcentage": "% du Total"
                },
                hide_index=True,
                use_container_width=True
            )
            
            # Add download capability
            st.download_button(
                label="📥 Télécharger l'Analyse des Suppressions",
                data=df_suppress.to_csv(index=False),
                file_name=f"analyse_archive_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
                help="Télécharger l'analyse complète des suppressions en CSV"
            )
            
            # Add informational note
            st.info(
                "📌 Note : Les documents sont marqués pour suppression lorsqu'ils ont plus de 20 ans (240 mois) "
                "depuis leur dernière mise à jour. Cette analyse montre la répartition de ces documents par "
                "type d'origine."
            )
        else:
            st.info("Aucun document n'est actuellement marqué pour suppression (pas de documents de plus de 20 ans).")
    else:
        st.warning("Les données du statut d'archivage ne sont pas disponibles.")
if __name__ == "__main__":
    main()