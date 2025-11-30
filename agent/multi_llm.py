"""
Multi-LLM support for schema extraction
Supports OpenAI, Anthropic, Google, and local models via Ollama
"""

import os
import json
from abc import ABC, abstractmethod
from typing import Optional
from rich.console import Console

console = Console()


class LLMProvider(ABC):
    """Base class for LLM providers"""
    
    @abstractmethod
    def extract_schema(self, content: str, system_prompt: str) -> dict:
        """Extract schema from content"""
        pass
    
    @abstractmethod
    def test_connection(self) -> bool:
        """Test connection to the LLM"""
        pass


class OpenAIProvider(LLMProvider):
    """OpenAI GPT models"""
    
    def __init__(self, model: str = "gpt-4o", api_key: Optional[str] = None):
        from openai import OpenAI
        self.client = OpenAI(api_key=api_key or os.getenv("OPENAI_API_KEY"))
        self.model = model
    
    def extract_schema(self, content: str, system_prompt: str) -> dict:
        response = self.client.chat.completions.create(
            model=self.model,
            temperature=0,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": content[:30000]}
            ]
        )
        return json.loads(response.choices[0].message.content)
    
    def test_connection(self) -> bool:
        try:
            self.client.models.list()
            return True
        except:
            return False


class AnthropicProvider(LLMProvider):
    """Anthropic Claude models"""
    
    def __init__(self, model: str = "claude-3-5-sonnet-20241022", api_key: Optional[str] = None):
        try:
            from anthropic import Anthropic
            self.client = Anthropic(api_key=api_key or os.getenv("ANTHROPIC_API_KEY"))
            self.model = model
            self.available = True
        except ImportError:
            self.available = False
            console.print("[yellow]Anthropic package not installed[/yellow]")
    
    def extract_schema(self, content: str, system_prompt: str) -> dict:
        if not self.available:
            raise RuntimeError("Anthropic not available")
        
        response = self.client.messages.create(
            model=self.model,
            max_tokens=4096,
            system=system_prompt + "\n\nReturn ONLY valid JSON.",
            messages=[
                {"role": "user", "content": content[:30000]}
            ]
        )
        
        # Extract JSON from response
        text = response.content[0].text
        # Try to find JSON in the response
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0]
        elif "```" in text:
            text = text.split("```")[1].split("```")[0]
        
        return json.loads(text.strip())
    
    def test_connection(self) -> bool:
        if not self.available:
            return False
        try:
            # Simple test message
            self.client.messages.create(
                model=self.model,
                max_tokens=10,
                messages=[{"role": "user", "content": "Hi"}]
            )
            return True
        except:
            return False


class GoogleProvider(LLMProvider):
    """Google Gemini models"""
    
    def __init__(self, model: str = "gemini-pro", api_key: Optional[str] = None):
        try:
            import google.generativeai as genai
            genai.configure(api_key=api_key or os.getenv("GOOGLE_API_KEY"))
            self.model = genai.GenerativeModel(model)
            self.available = True
        except ImportError:
            self.available = False
            console.print("[yellow]Google AI package not installed[/yellow]")
    
    def extract_schema(self, content: str, system_prompt: str) -> dict:
        if not self.available:
            raise RuntimeError("Google AI not available")
        
        prompt = f"{system_prompt}\n\nReturn ONLY valid JSON.\n\nContent:\n{content[:30000]}"
        response = self.model.generate_content(prompt)
        
        text = response.text
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0]
        elif "```" in text:
            text = text.split("```")[1].split("```")[0]
        
        return json.loads(text.strip())
    
    def test_connection(self) -> bool:
        if not self.available:
            return False
        try:
            self.model.generate_content("Hi")
            return True
        except:
            return False


class OllamaProvider(LLMProvider):
    """Local models via Ollama"""
    
    def __init__(self, model: str = "llama3.1", host: str = "http://localhost:11434"):
        self.model = model
        self.host = host
    
    def extract_schema(self, content: str, system_prompt: str) -> dict:
        import httpx
        
        response = httpx.post(
            f"{self.host}/api/generate",
            json={
                "model": self.model,
                "prompt": f"{system_prompt}\n\nReturn ONLY valid JSON.\n\nContent:\n{content[:15000]}",
                "stream": False,
                "options": {
                    "temperature": 0
                }
            },
            timeout=120
        )
        response.raise_for_status()
        
        text = response.json()["response"]
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0]
        elif "```" in text:
            text = text.split("```")[1].split("```")[0]
        
        return json.loads(text.strip())
    
    def test_connection(self) -> bool:
        import httpx
        try:
            response = httpx.get(f"{self.host}/api/tags", timeout=5)
            return response.status_code == 200
        except:
            return False


class MultiLLMExtractor:
    """
    Multi-LLM schema extractor with fallback support.
    Tries multiple providers in order until one succeeds.
    """
    
    def __init__(self, primary: str = "openai", fallbacks: Optional[list[str]] = None):
        self.providers: dict[str, LLMProvider] = {}
        self.primary = primary
        self.fallbacks = fallbacks or []
        
        # Initialize providers
        provider_classes = {
            "openai": lambda: OpenAIProvider(),
            "anthropic": lambda: AnthropicProvider(),
            "google": lambda: GoogleProvider(),
            "ollama": lambda: OllamaProvider()
        }
        
        for name in [primary] + self.fallbacks:
            if name in provider_classes:
                try:
                    self.providers[name] = provider_classes[name]()
                except Exception as e:
                    console.print(f"[yellow]Failed to initialize {name}: {e}[/yellow]")
    
    def extract_schema(
        self,
        content: str,
        system_prompt: str,
        source_type: str = "unknown",
        source_id: str = ""
    ) -> Optional[dict]:
        """
        Extract schema using available LLM providers with fallback
        
        Args:
            content: Text content to analyze
            system_prompt: System prompt for extraction
            source_type: Type of source (confluence, jira)
            source_id: Identifier for the source
            
        Returns:
            Extracted schema dictionary or None
        """
        providers_to_try = [self.primary] + self.fallbacks
        
        for provider_name in providers_to_try:
            if provider_name not in self.providers:
                continue
            
            provider = self.providers[provider_name]
            
            try:
                console.print(f"[cyan]Trying {provider_name}...[/cyan]")
                result = provider.extract_schema(content, system_prompt)
                
                # Add metadata
                result["source_type"] = source_type
                result["source_id"] = source_id
                result["extracted_by"] = provider_name
                
                console.print(f"[green]Successfully extracted with {provider_name}[/green]")
                return result
                
            except Exception as e:
                console.print(f"[yellow]{provider_name} failed: {e}[/yellow]")
                continue
        
        console.print("[red]All LLM providers failed[/red]")
        return None
    
    def get_available_providers(self) -> list[str]:
        """Get list of available and connected providers"""
        available = []
        for name, provider in self.providers.items():
            if provider.test_connection():
                available.append(name)
        return available
    
    def benchmark(self, test_content: str, system_prompt: str) -> dict:
        """
        Benchmark all available providers
        
        Returns:
            Dictionary with timing and success for each provider
        """
        import time
        results = {}
        
        for name, provider in self.providers.items():
            if not provider.test_connection():
                results[name] = {"available": False}
                continue
            
            try:
                start = time.time()
                result = provider.extract_schema(test_content, system_prompt)
                elapsed = time.time() - start
                
                results[name] = {
                    "available": True,
                    "success": True,
                    "time_seconds": round(elapsed, 2),
                    "tables_found": len(result.get("tables", []))
                }
            except Exception as e:
                results[name] = {
                    "available": True,
                    "success": False,
                    "error": str(e)
                }
        
        return results
