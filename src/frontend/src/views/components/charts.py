import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from typing import List, Dict, Any

class ChartDisplay:
    @staticmethod
    def create_document_distribution_chart(doc_counts: List[Dict[str, Any]], title: str) -> None:
        """
        Create and display a donut chart for document distribution.
        
        Args:
            doc_counts (List[Dict[str, Any]]): Document count data
            title (str): Chart title
        """
        df = pd.DataFrame(doc_counts)
        fig = px.pie(
            df,
            values="unique_document_count",
            names="document_origin_code",
            title=title,
            hole=0.4
        )
        fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig, use_container_width=True)
        
        total_docs = df['unique_document_count'].sum()
        st.metric("Documents Totaux", f"{total_docs:,}")

    @staticmethod
    def create_user_activity_chart(user_data: List[Dict[str, Any]], title: str) -> None:
        """
        Create and display a bar chart for user activity.
        
        Args:
            user_data (List[Dict[str, Any]]): User activity data
            title (str): Chart title
        """
        df = pd.DataFrame(user_data)
        fig = px.bar(
            df,
            x="lastname",
            y="query_count",
            color="query_count",
            text="query_count",
            title=title,
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
        
        st.plotly_chart(fig, use_container_width=True)

    @staticmethod
    def create_time_series_chart(data: List[Dict[str, Any]], 
                                time_column: str,
                                title: str,
                                show_range_selector: bool = True) -> None:
        """
        Create and display a time series line chart.
        
        Args:
            data (List[Dict[str, Any]]): Time series data
            time_column (str): Name of the time column
            title (str): Chart title
            show_range_selector (bool): Whether to show the range selector
        """
        df = pd.DataFrame(data)
        # Convert the 'year' integer to datetime at the beginning of the year
        df[time_column] = pd.to_datetime(df[time_column].astype(str) + '-01-01')
        
        fig = px.line(
            df,
            x=time_column,
            y="count",
            color="document_origin_code",
            title=title
        )
        
        fig.update_layout(
            xaxis_title="Date",
            yaxis_title="Nombre de Documents",
            legend_title="Origine du Document",
            hovermode='x unified'
        )
        
        if show_range_selector:
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
        
        st.plotly_chart(fig, use_container_width=True)

    @staticmethod
    def create_archive_chart(archive_data: Dict[str, Any]) -> None:
        """
        Create and display a bar chart for archive data.
        
        Args:
            archive_data (Dict[str, Any]): Archive status data
        """
        df = pd.DataFrame(
            archive_data['documents_to_suppress'],
            columns=['Origine', 'Nombre']
        )
        
        df['Pourcentage'] = (df['Nombre'] / df['Nombre'].sum() * 100).round(1)
        
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=df['Origine'],
            y=df['Nombre'],
            text=df.apply(lambda x: f"{x['Nombre']:,}<br>{x['Pourcentage']:.1f}%", axis=1),
            textposition='outside',
            marker_color='#FF4B4B',
            name='Documents à Supprimer'
        ))
        
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
        
        fig.update_traces(
            hovertemplate="<b>%{x}</b><br>" +
                         "Documents : %{y:,}<br>" +
                         "Pourcentage : %{text}<extra></extra>"
        )
        
        st.plotly_chart(fig, use_container_width=True)