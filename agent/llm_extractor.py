"""
LLM-powered schema extraction using OpenAI GPT-4o
"""

import os
import json
import re
from typing import Optional
from openai import OpenAI
from pydantic import BaseModel, Field
from rich.console import Console
from tenacity import retry, stop_after_attempt, wait_exponential

console = Console()


class Column(BaseModel):
    """Schema for a table column"""
    column_name: str
    data_type: Optional[str] = None
    description: str
    pii: bool = False
    nullable: bool = True
    primary_key: bool = False
    foreign_key: Optional[str] = None


class Table(BaseModel):
    """Schema for a database table"""
    table_name: str
    description: str
    owner: Optional[str] = None
    schema_name: Optional[str] = None
    columns: list[Column] = Field(default_factory=list)
    relationships: list[str] = Field(default_factory=list)


class ExtractedSchema(BaseModel):
    """Complete extracted schema"""
    tables: list[Table] = Field(default_factory=list)
    source_type: Optional[str] = None
    source_id: Optional[str] = None
    extraction_confidence: float = 0.0


class LLMExtractor:
    """Extract database schema information from unstructured text using GPT-4o"""
    
    def __init__(self):
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.model = "gpt-4o"
        self.prompts_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "prompts")
    
    def _load_prompt(self, prompt_name: str) -> str:
        """Load a prompt template from the prompts directory"""
        prompt_path = os.path.join(self.prompts_dir, prompt_name)
        with open(prompt_path, "r") as f:
            return f.read()
    
    def _clean_html(self, html_content: str) -> str:
        """Remove HTML tags and clean up content for LLM processing"""
        # Remove HTML tags
        clean = re.sub(r'<[^>]+>', ' ', html_content)
        # Remove extra whitespace
        clean = re.sub(r'\s+', ' ', clean)
        # Remove special characters but keep alphanumeric and punctuation
        clean = re.sub(r'[^\w\s.,;:!?\'"-]', '', clean)
        return clean.strip()
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def extract_schema(
        self, 
        content: str, 
        source_type: str = "unknown",
        source_id: str = ""
    ) -> Optional[dict]:
        """
        Extract schema information from content using GPT-4o
        
        Args:
            content: The text content to analyze (can be HTML)
            source_type: 'confluence' or 'jira'
            source_id: Page ID or Issue Key
            
        Returns:
            Extracted schema as a dictionary
        """
        # Clean content
        cleaned_content = self._clean_html(content)
        
        # Truncate if too long (GPT-4o context limit)
        max_chars = 30000
        if len(cleaned_content) > max_chars:
            cleaned_content = cleaned_content[:max_chars] + "... [truncated]"
        
        # Load prompt template
        system_prompt = self._load_prompt("extract_schema.txt")
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                temperature=0,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Source: {source_type} ({source_id})\n\nText to analyze:\n{cleaned_content}"}
                ]
            )
            
            result = json.loads(response.choices[0].message.content)
            
            # Add metadata
            result["source_type"] = source_type
            result["source_id"] = source_id
            
            # Validate with Pydantic
            schema = ExtractedSchema(**result)
            
            return schema.model_dump()
            
        except json.JSONDecodeError as e:
            console.print(f"[red]❌ Failed to parse LLM response as JSON: {e}[/red]")
            return None
        except Exception as e:
            console.print(f"[red]❌ LLM extraction error: {e}[/red]")
            raise
    
    def generate_column_comment(self, table_name: str, column_name: str, context: str) -> str:
        """Generate a more detailed comment for a specific column"""
        prompt = self._load_prompt("write_comments.txt")
        
        response = self.client.chat.completions.create(
            model=self.model,
            temperature=0,
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": f"Table: {table_name}\nColumn: {column_name}\nContext: {context}"}
            ]
        )
        
        return response.choices[0].message.content.strip()
    
    def enhance_descriptions(self, schema: dict) -> dict:
        """Enhance table and column descriptions with more business context"""
        enhanced = schema.copy()
        
        for table in enhanced.get("tables", []):
            # Generate enhanced table description
            if len(table.get("description", "")) < 50:
                context = f"Table {table['table_name']} with columns: {', '.join([c['column_name'] for c in table.get('columns', [])])}"
                table["description"] = self.generate_column_comment(
                    table["table_name"], 
                    "[TABLE]", 
                    context
                )
            
            # Enhance column descriptions
            for column in table.get("columns", []):
                if len(column.get("description", "")) < 20:
                    column["description"] = self.generate_column_comment(
                        table["table_name"],
                        column["column_name"],
                        table.get("description", "")
                    )
        
        return enhanced
