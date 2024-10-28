# schema_fetcher.py
from typing import Dict
import oracledb
from datetime import datetime

class OracleSchemaFetcher:
    def _fetch_tables(self, connection, schema_name: str) -> dict:
        """Fetch all tables with their metadata"""
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    t.TABLE_NAME,
                    tc.COMMENTS,
                    t.NUM_ROWS,
                    t.LAST_ANALYZED,
                    t.TABLESPACE_NAME,
                    t.PCT_FREE,
                    t.PCT_USED,
                    t.INI_TRANS,
                    t.MAX_TRANS,
                    t.COMPRESSION,
                    t.COMPRESS_FOR,
                    s.BYTES/1024/1024 as SIZE_MB
                FROM ALL_TABLES t
                LEFT JOIN ALL_TAB_COMMENTS tc 
                    ON t.TABLE_NAME = tc.TABLE_NAME
                    AND tc.OWNER = t.OWNER
                LEFT JOIN ALL_SEGMENTS s 
                    ON t.TABLE_NAME = s.SEGMENT_NAME
                    AND s.OWNER = t.OWNER
                WHERE t.OWNER = :owner
                ORDER BY t.TABLE_NAME
            """, {'owner': schema_name})
            
            columns = [col[0].lower() for col in cursor.description]
            tables = {}
            for row in cursor.fetchall():
                table_data = dict(zip(columns, row))
                tables[table_data['table_name']] = table_data
            return tables

    def _fetch_columns(self, connection, schema_name: str) -> dict:
        """Fetch all columns for all tables"""
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    c.TABLE_NAME,
                    c.COLUMN_NAME,
                    c.DATA_TYPE,
                    c.NULLABLE,
                    c.DATA_LENGTH,
                    c.DATA_PRECISION,
                    c.DATA_SCALE,
                    c.COLUMN_ID,
                    c.DEFAULT_LENGTH,
                    c.DATA_DEFAULT,
                    c.CHARACTER_SET_NAME,
                    cc.COMMENTS,
                    c.IDENTITY_COLUMN,
                    c.VIRTUAL_COLUMN
                FROM ALL_TAB_COLUMNS c
                LEFT JOIN ALL_COL_COMMENTS cc 
                    ON c.TABLE_NAME = cc.TABLE_NAME 
                    AND c.COLUMN_NAME = cc.COLUMN_NAME
                    AND c.OWNER = cc.OWNER
                WHERE c.OWNER = :owner
                ORDER BY c.TABLE_NAME, c.COLUMN_ID
            """, {'owner': schema_name})
            
            columns = [col[0].lower() for col in cursor.description]
            result = {}
            for row in cursor.fetchall():
                data = dict(zip(columns, row))
                table_name = data.pop('table_name')
                if table_name not in result:
                    result[table_name] = []
                result[table_name].append(data)
            return result

    def _fetch_constraints(self, connection, schema_name: str) -> dict:
        """Fetch all constraints and relationships"""
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    ac.TABLE_NAME,
                    ac.CONSTRAINT_NAME,
                    ac.CONSTRAINT_TYPE,
                    ac.R_OWNER,
                    ac.R_CONSTRAINT_NAME,
                    ac.DELETE_RULE,
                    ac.STATUS,
                    acc.COLUMN_NAME,
                    acc.POSITION,
                    rc.TABLE_NAME as R_TABLE_NAME,
                    rcc.COLUMN_NAME as R_COLUMN_NAME
                FROM ALL_CONSTRAINTS ac
                JOIN ALL_CONS_COLUMNS acc 
                    ON ac.CONSTRAINT_NAME = acc.CONSTRAINT_NAME
                    AND ac.OWNER = acc.OWNER
                LEFT JOIN ALL_CONSTRAINTS rc 
                    ON ac.R_CONSTRAINT_NAME = rc.CONSTRAINT_NAME
                    AND ac.R_OWNER = rc.OWNER
                LEFT JOIN ALL_CONS_COLUMNS rcc 
                    ON rc.CONSTRAINT_NAME = rcc.CONSTRAINT_NAME
                    AND rc.OWNER = rcc.OWNER
                    AND acc.POSITION = rcc.POSITION
                WHERE ac.OWNER = :owner
                ORDER BY ac.TABLE_NAME, ac.CONSTRAINT_NAME, acc.POSITION
            """, {'owner': schema_name})
            
            columns = [col[0].lower() for col in cursor.description]
            constraints = {}
            relationships = {}
            
            for row in cursor.fetchall():
                data = dict(zip(columns, row))
                table_name = data['table_name']
                
                # Store as constraint
                if table_name not in constraints:
                    constraints[table_name] = []
                constraints[table_name].append(data)
                
                # If it's a foreign key, store as relationship
                if data['constraint_type'] == 'R' and data['r_table_name']:
                    rel_key = f"{table_name}_{data['r_table_name']}_{data['constraint_name']}"
                    relationships[rel_key] = {
                        'table_name': table_name,
                        'column_name': data['column_name'],
                        'r_table_name': data['r_table_name'],
                        'r_column_name': data['r_column_name'],
                        'delete_rule': data['delete_rule']
                    }
            
            return {'constraints': constraints, 'relationships': relationships}

    def _fetch_indexes(self, connection, schema_name: str) -> dict:
        """Fetch all indexes for all tables"""
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    i.TABLE_NAME,
                    i.INDEX_NAME,
                    i.INDEX_TYPE,
                    i.UNIQUENESS,
                    i.COMPRESSION,
                    i.TABLESPACE_NAME,
                    i.STATUS,
                    ic.COLUMN_NAME,
                    ic.COLUMN_POSITION,
                    ic.DESCEND
                FROM ALL_INDEXES i
                JOIN ALL_IND_COLUMNS ic 
                    ON i.INDEX_NAME = ic.INDEX_NAME
                    AND i.OWNER = ic.INDEX_OWNER
                WHERE i.OWNER = :owner
                ORDER BY i.TABLE_NAME, i.INDEX_NAME, ic.COLUMN_POSITION
            """, {'owner': schema_name})
            
            columns = [col[0].lower() for col in cursor.description]
            result = {}
            for row in cursor.fetchall():
                data = dict(zip(columns, row))
                table_name = data.pop('table_name')
                if table_name not in result:
                    result[table_name] = []
                result[table_name].append(data)
            return result

    def fetch_all_metadata(self, connection, schema_name: str) -> dict:
        """Fetch all metadata in a single function call"""
        tables = self._fetch_tables(connection, schema_name)
        columns = self._fetch_columns(connection, schema_name)
        constraints_and_relationships = self._fetch_constraints(connection, schema_name)
        indexes = self._fetch_indexes(connection, schema_name)
        
        return {
            'tables': tables,
            'columns': columns,
            'constraints': constraints_and_relationships['constraints'],
            'relationships': constraints_and_relationships['relationships'],
            'indexes': indexes,
            'last_updated': datetime.now().isoformat(),
            'schema_name': schema_name
        }

# app/core/cache/manager.py
from redis import Redis
import pickle
from datetime import datetime
import os
import oracledb
from schema_fetcher import OracleSchemaFetcher

class SchemaCache:
    def __init__(self):
        self.redis = Redis(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            password=os.getenv('REDIS_PASSWORD', None),
            decode_responses=True
        )
        self.binary_redis = Redis(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            password=os.getenv('REDIS_PASSWORD', None),
            decode_responses=False
        )
        self.cache_ttl = 24 * 60 * 60  # 24 hours
        self.fetcher = OracleSchemaFetcher()

    def get_cache_key(self, schema_name: str) -> str:
        return f"oracle_schema:{schema_name}:metadata:{datetime.now().strftime('%Y-%m-%d')}"

    def get_schema_metadata(self, schema_name: str) -> dict:
        """Get schema metadata from cache or database"""
        cache_key = self.get_cache_key(schema_name)
        
        # Try to get from cache
        cached_data = self.binary_redis.get(cache_key)
        if cached_data:
            return pickle.loads(cached_data)

        # If not in cache, fetch from database
        with oracledb.connect(
            user=os.getenv("ORACLE_USER"),
            password=os.getenv("ORACLE_PASSWORD"),
            dsn=os.getenv("ORACLE_DSN")
        ) as connection:
            metadata = self.fetcher.fetch_all_metadata(connection, schema_name)
        
        # Store in cache
        self.binary_redis.setex(
            cache_key,
            self.cache_ttl,
            pickle.dumps(metadata)
        )
        
        return metadata

# app/core/cache/dependencies.py
from fastapi import Depends
from .manager import SchemaCache

schema_cache = SchemaCache()

def get_schema_cache() -> SchemaCache:
    return schema_cache

# app/api/v1/routes/schema.py
from fastapi import APIRouter, Depends, HTTPException
from typing import Dict
from app.core.cache.dependencies import get_schema_cache
from app.schemas.schema import SchemaResponse

router = APIRouter()

@router.get("/{schema_name}", response_model=SchemaResponse)
async def get_schema_metadata(
    schema_name: str,
    cache: SchemaCache = Depends(get_schema_cache)
):
    try:
        return cache.get_schema_metadata(schema_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# app/schemas/schema.py
from pydantic import BaseModel
from typing import Dict, List, Optional
from datetime import datetime

class TableMetadata(BaseModel):
    table_name: str
    comments: Optional[str]
    num_rows: Optional[int]
    last_analyzed: Optional[datetime]
    tablespace_name: Optional[str]
    # ... other fields

class SchemaResponse(BaseModel):
    tables: Dict[str, TableMetadata]
    columns: Dict[str, List[Dict]]
    constraints: Dict[str, List[Dict]]
    relationships: Dict[str, Dict]
    indexes: Dict[str, List[Dict]]
    last_updated: datetime
    schema_name: str

# backend main.py
from fastapi import FastAPI
from app.api.v1.endpoints import schema
from app.core.config import settings

app = FastAPI(title=settings.PROJECT_NAME)

app.include_router(
    schema.router,
    prefix="/api/v1/schema",
    tags=["schema"]
)

# streamlit_app.py
import streamlit as st
import requests
from datetime import datetime
import plotly.graph_objects as go
import networkx as nx
import pandas as pd

def load_schema_metadata():
    """Load schema metadata from cache or fetch new data"""
    if 'schema_metadata' not in st.session_state or \
       'last_loaded_date' not in st.session_state or \
       st.session_state.last_loaded_date.date() != datetime.now().date():
        
        schema_name = os.getenv('ORACLE_SCHEMA')
        response = requests.get(f"{API_URL}/schema/metadata/{schema_name}")
        
        if response.status_code == 200:
            st.session_state.schema_metadata = response.json()
            st.session_state.last_loaded_date = datetime.now()
            st.session_state.graph = create_relationship_graph(
                st.session_state.schema_metadata['relationships']
            )
        else:
            st.error("Failed to load schema metadata")
            return None
    
    return st.session_state.schema_metadata

def create_relationship_graph(relationships: Dict):
    """Create network graph from relationships"""
    G = nx.DiGraph()
    
    # Add nodes and edges
    for rel in relationships.values():
        G.add_edge(
            rel['table_name'],
            rel['r_table_name'],
            key=f"{rel['column_name']} → {rel['r_column_name']}"
        )
    
    return G

def main():
    st.set_page_config(layout="wide")
    st.title("Oracle Schema Explorer")
    
    # Load cached metadata
    metadata = load_schema_metadata()
    if not metadata:
        return

    # Sidebar for filtering
    st.sidebar.title("Navigation")
    search_term = st.sidebar.text_input("Search Tables").lower()
    
    # Filter tables based on search
    filtered_tables = {
        name: data for name, data in metadata['tables'].items()
        if search_term in name.lower()
    }
    
    # Table selection
    selected_table = st.sidebar.selectbox(
        "Select Table",
        options=list(filtered_tables.keys()),
        format_func=lambda x: f"{x} ({filtered_tables[x]['num_rows']:,} rows)"
    )

    if selected_table:
        # Main content area with tabs
        tab1, tab2, tab3 = st.tabs(["Table Details", "Relationships", "Storage"])
        
        with tab1:
            display_table_details(selected_table, metadata)
        
        with tab2:
            display_relationships(selected_table, metadata)
        
        with tab3:
            display_storage_info(selected_table, metadata)

def display_table_details(table_name: str, metadata: Dict):
    """Display detailed table information"""
    table_meta = metadata['tables'][table_name]
    columns = metadata['columns'].get(table_name, {})
    
    st.header(f"Table: {table_name}")
    
    if table_meta.get('comments'):
        st.info(table_meta['comments'])
    
    # Display columns in a clean table
    st.subheader("Columns")
    columns_df = pd.DataFrame(columns)
    st.dataframe(
        columns_df[['column_name', 'data_type', 'nullable', 'comments']],
        use_container_width=True
    )
    
    # Show constraints if any exist
    constraints = metadata['constraints'].get(table_name, {})
    if constraints:
        st.subheader("Constraints")
        st.dataframe(pd.DataFrame(constraints))

def display_relationships(table_name: str, metadata: Dict):
    """Display relationship visualization"""
    st.plotly_chart(
        create_network_visualization(
            st.session_state.graph,
            center_node=table_name
        ),
        use_container_width=True
    )

def display_storage_info(table_name: str, metadata: Dict):
    """Display storage information"""
    storage = metadata['storage'].get(table_name, {})
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Size (MB)", f"{storage.get('size_mb', 0):.2f}")
        st.metric("Tablespace", storage.get('tablespace_name', 'N/A'))
    
    with col2:
        st.metric("Compression", storage.get('compression', 'N/A'))
        st.metric("Last Analyzed", storage.get('last_analyzed', 'N/A'))

if __name__ == "__main__":
    main()