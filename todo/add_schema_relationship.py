# fastapi_backend.py
from fastapi import FastAPI, HTTPException
from typing import List, Dict, Optional
import oracledb
import os
from pydantic import BaseModel

app = FastAPI()

class TableMetadata(BaseModel):
    table_name: str
    comments: Optional[str]
    num_rows: Optional[int]
    last_analyzed: Optional[str]
    tablespace_name: Optional[str]

class ColumnMetadata(BaseModel):
    column_name: str
    data_type: str
    nullable: str
    data_length: int
    data_precision: Optional[int]
    data_scale: Optional[int]
    comments: Optional[str]

class Relationship(BaseModel):
    constraint_name: str
    table_name: str
    column_name: str
    r_table_name: str
    r_column_name: str

def get_db_connection():
    return oracledb.connect(
        user=os.getenv("ORACLE_USER"),
        password=os.getenv("ORACLE_PASSWORD"),
        dsn=os.getenv("ORACLE_DSN")
    )

@app.get("/schema/tables", response_model=List[TableMetadata])
async def get_tables():
    """Get all tables with their metadata"""
    with get_db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    t.TABLE_NAME,
                    tc.COMMENTS,
                    t.NUM_ROWS,
                    t.LAST_ANALYZED,
                    t.TABLESPACE_NAME
                FROM ALL_TABLES t
                LEFT JOIN ALL_TAB_COMMENTS tc 
                    ON t.TABLE_NAME = tc.TABLE_NAME
                WHERE t.OWNER = :owner
                ORDER BY t.TABLE_NAME
            """, owner=os.getenv("ORACLE_SCHEMA"))
            
            columns = [col[0].lower() for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

@app.get("/schema/table/{table_name}/columns", response_model=List[ColumnMetadata])
async def get_table_columns(table_name: str):
    """Get detailed column information for a specific table"""
    with get_db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    c.COLUMN_NAME,
                    c.DATA_TYPE,
                    c.NULLABLE,
                    c.DATA_LENGTH,
                    c.DATA_PRECISION,
                    c.DATA_SCALE,
                    cc.COMMENTS
                FROM ALL_TAB_COLUMNS c
                LEFT JOIN ALL_COL_COMMENTS cc 
                    ON c.TABLE_NAME = cc.TABLE_NAME 
                    AND c.COLUMN_NAME = cc.COLUMN_NAME
                WHERE c.TABLE_NAME = :table_name
                AND c.OWNER = :owner
                ORDER BY c.COLUMN_ID
            """, table_name=table_name, owner=os.getenv("ORACLE_SCHEMA"))
            
            columns = [col[0].lower() for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

@app.get("/schema/table/{table_name}/constraints")
async def get_table_constraints(table_name: str):
    """Get all constraints for a specific table"""
    with get_db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    ac.CONSTRAINT_NAME,
                    ac.CONSTRAINT_TYPE,
                    acc.COLUMN_NAME,
                    ac.R_CONSTRAINT_NAME,
                    ac.DELETE_RULE
                FROM ALL_CONSTRAINTS ac
                JOIN ALL_CONS_COLUMNS acc 
                    ON ac.CONSTRAINT_NAME = acc.CONSTRAINT_NAME
                WHERE ac.TABLE_NAME = :table_name
                AND ac.OWNER = :owner
            """, table_name=table_name, owner=os.getenv("ORACLE_SCHEMA"))
            
            columns = [col[0].lower() for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

@app.get("/schema/relationships", response_model=List[Relationship])
async def get_relationships():
    """Get all relationships between tables"""
    with get_db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    ac.CONSTRAINT_NAME,
                    ac.TABLE_NAME,
                    acc.COLUMN_NAME,
                    arc.TABLE_NAME as R_TABLE_NAME,
                    arc.COLUMN_NAME as R_COLUMN_NAME
                FROM ALL_CONSTRAINTS ac
                JOIN ALL_CONS_COLUMNS acc 
                    ON ac.CONSTRAINT_NAME = acc.CONSTRAINT_NAME
                JOIN ALL_CONS_COLUMNS arc 
                    ON ac.R_CONSTRAINT_NAME = arc.CONSTRAINT_NAME
                WHERE ac.CONSTRAINT_TYPE = 'R'
                AND ac.OWNER = :owner
            """, owner=os.getenv("ORACLE_SCHEMA"))
            
            columns = [col[0].lower() for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

@app.get("/schema/table/{table_name}/indexes")
async def get_table_indexes(table_name: str):
    """Get all indexes for a specific table"""
    with get_db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    ai.INDEX_NAME,
                    ai.INDEX_TYPE,
                    ai.UNIQUENESS,
                    aic.COLUMN_NAME,
                    aic.COLUMN_POSITION
                FROM ALL_INDEXES ai
                JOIN ALL_IND_COLUMNS aic 
                    ON ai.INDEX_NAME = aic.INDEX_NAME
                WHERE ai.TABLE_NAME = :table_name
                AND ai.OWNER = :owner
                ORDER BY aic.COLUMN_POSITION
            """, table_name=table_name, owner=os.getenv("ORACLE_SCHEMA"))
            
            columns = [col[0].lower() for col in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

@app.get("/schema/table/{table_name}/storage")
async def get_table_storage(table_name: str):
    """Get storage information for a specific table"""
    with get_db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    t.TABLESPACE_NAME,
                    t.PCT_FREE,
                    t.PCT_USED,
                    t.INI_TRANS,
                    t.MAX_TRANS,
                    s.BYTES/1024/1024 as SIZE_MB,
                    t.COMPRESSION,
                    t.COMPRESS_FOR
                FROM ALL_TABLES t
                JOIN ALL_SEGMENTS s 
                    ON t.TABLE_NAME = s.SEGMENT_NAME
                WHERE t.TABLE_NAME = :table_name
                AND t.OWNER = :owner
            """, table_name=table_name, owner=os.getenv("ORACLE_SCHEMA"))
            
            columns = [col[0].lower() for col in cursor.description]
            return dict(zip(columns, cursor.fetchone()))

# Streamlit frontend adjustments for Oracle
import streamlit as st
import requests
import networkx as nx
import plotly.graph_objects as go
import pandas as pd

def create_relationship_graph(relationships, selected_table=None, depth=2):
    """
    Create a network graph visualization of table relationships
    
    Args:
        relationships (list): List of relationship dictionaries
        selected_table (str, optional): Table to focus on
        depth (int, optional): How many levels of relationships to show from selected table
    
    Returns:
        plotly.graph_objects.Figure: Interactive graph visualization
    """

    # Create directed graph
    G = nx.DiGraph()
    
    # Add all relationships to the graph
    for rel in relationships:
        source = rel['table_name']
        target = rel['r_table_name']
        G.add_edge(
            source, 
            target, 
            label=f"{rel['column_name']} → {rel['r_column_name']}"
        )
    
    # If a table is selected, limit the graph to nearby tables
    if selected_table and selected_table in G:
        # Get tables within specified depth
        nodes_in_range = set()
        current_nodes = {selected_table}
        
        for _ in range(depth):
            next_nodes = set()
            for node in current_nodes:
                # Add successors (tables this one references)
                next_nodes.update(G.successors(node))
                # Add predecessors (tables that reference this one)
                next_nodes.update(G.predecessors(node))
            nodes_in_range.update(current_nodes)
            current_nodes = next_nodes - nodes_in_range
        
        # Create subgraph with only the nodes in range
        nodes_in_range.update(current_nodes)
        G = G.subgraph(nodes_in_range)
    
    # Calculate layout
    pos = nx.spring_layout(G, k=1/float(len(G.nodes())**0.5), iterations=50)
    
    # Create edges
    edge_trace = []
    for edge in G.edges(data=True):
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        
        # Create arrow shape using bezier curve
        edge_trace.append(
            go.Scatter(
                x=[x0, (x0+x1)/2, x1],
                y=[y0, (y0+y1)/2 + 0.1, y1],
                mode='lines+text',
                line=dict(width=1, color='#888'),
                hoverinfo='text',
                text=edge[2]['label'],
                textposition='middle',
                showlegend=False
            )
        )
    
    # Create nodes
    node_x = []
    node_y = []
    node_text = []
    node_color = []
    
    for node in G.nodes():
        x, y = pos[node]
        node_x.append(x)
        node_y.append(y)
        node_text.append(node)
        # Highlight selected table
        if node == selected_table:
            node_color.append('#1f77b4')  # Blue for selected
        else:
            node_color.append('#95a5a6')  # Gray for others
    
    node_trace = go.Scatter(
        x=node_x,
        y=node_y,
        mode='markers+text',
        hoverinfo='text',
        text=node_text,
        textposition="bottom center",
        marker=dict(
            size=30,
            color=node_color,
            line_width=2,
            line=dict(color='white')
        )
    )
    
    # Create figure
    fig = go.Figure(
        data=[*edge_trace, node_trace],
        layout=go.Layout(
            showlegend=False,
            hovermode='closest',
            margin=dict(b=20, l=5, r=5, t=40),
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            plot_bgcolor='white',
            height=600,
            title=dict(
                text='Table Relationships' + (f' (from {selected_table})' if selected_table else ''),
                x=0.5,
                y=0.95
            )
        )
    )
    
    # Add buttons for interactivity
    fig.update_layout(
        updatemenus=[
            dict(
                type="buttons",
                showactive=False,
                buttons=[
                    dict(
                        label="Reset View",
                        method="relayout",
                        args=[{"xaxis.range": None, "yaxis.range": None}]
                    )
                ],
                x=0.05,
                y=1,
                xanchor="left",
                yanchor="top",
            )
        ]
    )
    
    return fig

API_URL = "test"

def main():
    st.title("Oracle Database Schema Explorer")
    
    # Sidebar for table selection
    tables_response = requests.get(f"{API_URL}/schema/tables")
    tables = pd.DataFrame(tables_response.json())
    
    selected_table = st.sidebar.selectbox(
        "Select Table",
        tables['table_name'].tolist(),
        format_func=lambda x: f"{x} ({tables[tables['table_name']==x]['num_rows'].iloc[0]:,} rows)"
    )
    
    # Main content tabs
    tab1, tab2, tab3 = st.tabs(["Table Details", "Relationships", "Storage"])
    
    with tab1:
        if selected_table:
            # Table metadata
            table_meta = tables[tables['table_name'] == selected_table].iloc[0]
            st.header(f"Table: {selected_table}")
            
            if table_meta['comments']:
                st.info(table_meta['comments'])
            
            # Columns
            cols_response = requests.get(f"{API_URL}/schema/table/{selected_table}/columns")
            cols_df = pd.DataFrame(cols_response.json())
            
            st.subheader("Columns")
            st.dataframe(cols_df[['column_name', 'data_type', 'nullable', 'comments']])
            
            # Constraints
            constraints_response = requests.get(f"{API_URL}/schema/table/{selected_table}/constraints")
            if constraints_response.json():
                st.subheader("Constraints")
                cons_df = pd.DataFrame(constraints_response.json())
                st.dataframe(cons_df)
            
            # Indexes
            indexes_response = requests.get(f"{API_URL}/schema/table/{selected_table}/indexes")
            if indexes_response.json():
                st.subheader("Indexes")
                idx_df = pd.DataFrame(indexes_response.json())
                st.dataframe(idx_df)
    
    with tab2:
        st.subheader("Table Relationships")
        relationships = requests.get(f"{API_URL}/schema/relationships").json()
        graph = create_relationship_graph(relationships, selected_table)
        st.plotly_chart(graph, use_container_width=True)
    
    with tab3:
        st.subheader("Storage Information")
        storage_info = requests.get(f"{API_URL}/schema/table/{selected_table}/storage").json()
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric("Size (MB)", f"{storage_info['size_mb']:.2f}")
            st.metric("Tablespace", storage_info['tablespace_name'])
        
        with col2:
            st.metric("Compression", storage_info['compression'])
            if storage_info['compress_for']:
                st.metric("Compress For", storage_info['compress_for'])

if __name__ == "__main__":
    main()