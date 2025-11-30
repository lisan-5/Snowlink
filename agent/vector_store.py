"""
Vector store for semantic search across documentation using ChromaDB
Enables intelligent retrieval of similar schemas and documentation
"""

import os
import hashlib
from datetime import datetime
from typing import Optional
import json
from openai import OpenAI
from pydantic import BaseModel, Field
from rich.console import Console

console = Console()

# ChromaDB for local vector storage (no external DB needed)
try:
    import chromadb
    from chromadb.config import Settings
    CHROMADB_AVAILABLE = True
except ImportError:
    CHROMADB_AVAILABLE = False
    console.print("[yellow]ChromaDB not installed. Vector search disabled.[/yellow]")


class DocumentChunk(BaseModel):
    """A chunk of documentation with metadata"""
    id: str
    content: str
    source_type: str  # confluence, jira, snowflake
    source_id: str
    title: Optional[str] = None
    tables_mentioned: list[str] = Field(default_factory=list)
    created_at: str = Field(default_factory=lambda: datetime.now().isoformat())
    embedding: Optional[list[float]] = None


class VectorStore:
    """
    Semantic search over documentation using embeddings.
    Enables finding similar schemas, related documentation, and intelligent retrieval.
    """
    
    def __init__(self, persist_directory: str = "data/vector_store"):
        self.persist_directory = persist_directory
        self.openai = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.embedding_model = "text-embedding-3-small"
        self.collection_name = "snowlink_docs"
        
        if CHROMADB_AVAILABLE:
            os.makedirs(persist_directory, exist_ok=True)
            self.client = chromadb.PersistentClient(
                path=persist_directory,
                settings=Settings(anonymized_telemetry=False)
            )
            self.collection = self.client.get_or_create_collection(
                name=self.collection_name,
                metadata={"hnsw:space": "cosine"}
            )
        else:
            self.client = None
            self.collection = None
    
    def _generate_embedding(self, text: str) -> list[float]:
        """Generate embedding for text using OpenAI"""
        response = self.openai.embeddings.create(
            model=self.embedding_model,
            input=text[:8000]  # Truncate to fit token limit
        )
        return response.data[0].embedding
    
    def _generate_id(self, source_type: str, source_id: str, chunk_index: int = 0) -> str:
        """Generate a unique ID for a document chunk"""
        content = f"{source_type}:{source_id}:{chunk_index}"
        return hashlib.md5(content.encode()).hexdigest()
    
    def add_document(
        self,
        content: str,
        source_type: str,
        source_id: str,
        title: Optional[str] = None,
        tables_mentioned: Optional[list[str]] = None,
        chunk_size: int = 1500
    ) -> int:
        """
        Add a document to the vector store with automatic chunking
        
        Returns:
            Number of chunks added
        """
        if not CHROMADB_AVAILABLE:
            return 0
        
        # Split content into chunks
        chunks = self._split_into_chunks(content, chunk_size)
        
        documents = []
        embeddings = []
        metadatas = []
        ids = []
        
        for i, chunk in enumerate(chunks):
            doc_id = self._generate_id(source_type, source_id, i)
            
            # Generate embedding
            embedding = self._generate_embedding(chunk)
            
            documents.append(chunk)
            embeddings.append(embedding)
            metadatas.append({
                "source_type": source_type,
                "source_id": source_id,
                "title": title or "",
                "tables_mentioned": json.dumps(tables_mentioned or []),
                "chunk_index": i,
                "created_at": datetime.now().isoformat()
            })
            ids.append(doc_id)
        
        # Upsert to collection
        self.collection.upsert(
            ids=ids,
            documents=documents,
            embeddings=embeddings,
            metadatas=metadatas
        )
        
        console.print(f"[green]Added {len(chunks)} chunks from {source_type}:{source_id}[/green]")
        return len(chunks)
    
    def _split_into_chunks(self, text: str, chunk_size: int) -> list[str]:
        """Split text into overlapping chunks"""
        words = text.split()
        chunks = []
        overlap = chunk_size // 4
        
        i = 0
        while i < len(words):
            chunk_words = words[i:i + chunk_size]
            chunks.append(" ".join(chunk_words))
            i += chunk_size - overlap
        
        return chunks if chunks else [text]
    
    def search(
        self,
        query: str,
        n_results: int = 5,
        source_type: Optional[str] = None,
        min_similarity: float = 0.7
    ) -> list[dict]:
        """
        Search for similar documents
        
        Args:
            query: The search query
            n_results: Number of results to return
            source_type: Filter by source type (confluence, jira, snowflake)
            min_similarity: Minimum cosine similarity threshold
            
        Returns:
            List of matching documents with metadata
        """
        if not CHROMADB_AVAILABLE:
            return []
        
        # Generate query embedding
        query_embedding = self._generate_embedding(query)
        
        # Build filter
        where_filter = None
        if source_type:
            where_filter = {"source_type": source_type}
        
        # Search
        results = self.collection.query(
            query_embeddings=[query_embedding],
            n_results=n_results,
            where=where_filter,
            include=["documents", "metadatas", "distances"]
        )
        
        # Process results
        matches = []
        for i, doc in enumerate(results["documents"][0]):
            distance = results["distances"][0][i]
            similarity = 1 - distance  # Convert distance to similarity
            
            if similarity >= min_similarity:
                metadata = results["metadatas"][0][i]
                matches.append({
                    "content": doc,
                    "similarity": round(similarity, 3),
                    "source_type": metadata["source_type"],
                    "source_id": metadata["source_id"],
                    "title": metadata.get("title", ""),
                    "tables_mentioned": json.loads(metadata.get("tables_mentioned", "[]"))
                })
        
        return matches
    
    def find_related_tables(self, table_name: str, n_results: int = 10) -> list[dict]:
        """Find documentation related to a specific table"""
        query = f"table {table_name} columns schema definition"
        return self.search(query, n_results=n_results)
    
    def find_similar_schemas(self, schema: dict, n_results: int = 5) -> list[dict]:
        """Find documentation with similar schemas"""
        # Build query from schema
        table_names = [t["table_name"] for t in schema.get("tables", [])]
        column_names = []
        for table in schema.get("tables", []):
            column_names.extend([c["column_name"] for c in table.get("columns", [])])
        
        query = f"tables: {', '.join(table_names)} columns: {', '.join(column_names[:20])}"
        return self.search(query, n_results=n_results)
    
    def get_stats(self) -> dict:
        """Get statistics about the vector store"""
        if not CHROMADB_AVAILABLE:
            return {"available": False}
        
        return {
            "available": True,
            "total_documents": self.collection.count(),
            "persist_directory": self.persist_directory
        }
    
    def delete_by_source(self, source_type: str, source_id: str) -> int:
        """Delete all chunks for a specific source"""
        if not CHROMADB_AVAILABLE:
            return 0
        
        # Find all IDs for this source
        results = self.collection.get(
            where={"source_type": source_type, "source_id": source_id}
        )
        
        if results["ids"]:
            self.collection.delete(ids=results["ids"])
            return len(results["ids"])
        
        return 0
