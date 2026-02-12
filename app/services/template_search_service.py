"""
🔍 Template Search Service
Busca semântica de templates baseado no prompt do usuário.

Usa:
- Matching por keywords
- Matching por categorias
- Matching por mood
- Similaridade com example_prompts

Fonte da verdade: video_editing_templates (banco de dados)
Fallback: template-catalog.json (arquivo estático)
"""

import json
import os
import re
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

# Caminho para o catálogo de templates (fallback)
CATALOG_PATH = os.path.join(
    os.path.dirname(__file__),
    "..", "data", "template-master-v3", "items", "template-catalog.json"
)


@dataclass
class TemplateMatch:
    """Resultado de uma busca de template."""
    template_id: str
    name: str
    description_short: str
    score: float
    match_reasons: List[str]
    colors: Dict[str, str]
    categories: List[str]
    mood: List[str]
    thumbnail_url: str = ""
    preview_image_url: str = ""
    video_url: str = ""


class TemplateSearchService:
    """Serviço de busca semântica de templates."""
    
    _instance = None
    _templates = None
    _categories = None
    _use_database = True
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._templates is None:
            self._load_templates()
    
    def _load_templates(self):
        """Carrega templates do banco de dados ou fallback para JSON."""
        if self._use_database:
            try:
                self._load_from_database()
                if self._templates:
                    logger.info(f"✅ Templates loaded from database: {len(self._templates)}")
                    return
            except Exception as e:
                logger.warning(f"⚠️ Failed to load templates from database: {e}")
        
        # Fallback para JSON
        self._load_from_json()
    
    def _load_from_database(self):
        """Carrega templates aprovados do banco de dados."""
        from app.db import get_db_cursor
        
        with get_db_cursor() as cur:
            cur.execute("""
                SELECT 
                    id, name, description, tags, metadata,
                    thumbnail_url, preview_image_url, video_url,
                    ts_default_font, ts_default_highlight
                FROM video_editing_templates 
                WHERE approved = TRUE AND is_active = TRUE
                ORDER BY created_at DESC
            """)
            rows = cur.fetchall()
        
        self._templates = []
        for row in rows:
            metadata = row["metadata"] or {}
            
            template = {
                "template_id": str(row["id"]),
                "name": row["name"],
                "description_short": row["description"] or "",
                "description_long": row["description"] or "",
                "categories": metadata.get("categories", []),
                "keywords": metadata.get("keywords", row["tags"] or []),
                "best_for": metadata.get("best_for", []),
                "NOT_for": metadata.get("NOT_for", []),
                "mood": metadata.get("mood", []),
                "colors": metadata.get("colors", {}),
                "example_prompts": metadata.get("example_prompts", []),
                "popularity_score": metadata.get("popularity_score", 50),
                "hashtag": metadata.get("hashtag", ""),
                "marketing_code": metadata.get("marketing_code", ""),
                "display_name": metadata.get("display_name", row["name"]),
                "thumbnail_url": row.get("thumbnail_url", ""),
                "preview_image_url": row.get("preview_image_url", ""),
                "video_url": row.get("video_url", ""),
                "font_config": row.get("ts_default_font", {}),
                "highlight_config": row.get("ts_default_highlight", {}),
                "status": "active"
            }
            self._templates.append(template)
        
        # Carregar categorias do JSON (ainda não temos no banco)
        self._load_categories_from_json()
    
    def _load_from_json(self):
        """Carrega do arquivo JSON (fallback)."""
        try:
            with open(CATALOG_PATH, 'r', encoding='utf-8') as f:
                catalog = json.load(f)
            self._templates = catalog.get('templates', [])
            self._categories = catalog.get('categories', [])
            logger.info(f"✅ Templates loaded from JSON: {len(self._templates)}")
        except FileNotFoundError:
            logger.error(f"❌ Template catalog not found: {CATALOG_PATH}")
            self._templates = []
            self._categories = []
        except json.JSONDecodeError as e:
            logger.error(f"❌ Invalid JSON in template catalog: {e}")
            self._templates = []
            self._categories = []
    
    def _load_categories_from_json(self):
        """Carrega apenas categorias do JSON."""
        try:
            with open(CATALOG_PATH, 'r', encoding='utf-8') as f:
                catalog = json.load(f)
            self._categories = catalog.get('categories', [])
        except Exception:
            self._categories = [
                {"id": "professional", "name": "Profissional", "keywords": ["profissional", "corporativo", "trabalho"]},
                {"id": "entertainment", "name": "Entretenimento", "keywords": ["divertido", "viral", "tiktok", "reels"]},
                {"id": "creative", "name": "Criativo", "keywords": ["criativo", "artístico", "original"]},
                {"id": "personal", "name": "Pessoal", "keywords": ["pessoal", "lifestyle", "casual"]},
                {"id": "educational", "name": "Educacional", "keywords": ["educacional", "aula", "curso", "tutorial"]}
            ]
    
    def reload_templates(self):
        """Recarrega templates (útil após edições no banco)."""
        self._templates = None
        self._load_templates()
    
    def search(
        self, 
        query: str, 
        max_results: int = 5,
        min_score: float = 0.3
    ) -> List[TemplateMatch]:
        """
        Busca templates que correspondem ao query.
        
        Args:
            query: Texto do usuário descrevendo o que quer
            max_results: Máximo de resultados
            min_score: Score mínimo para incluir (0-1)
        
        Returns:
            Lista de TemplateMatch ordenados por score
        """
        if not self._templates:
            return []
        
        # Tokenizar query
        tokens = self._tokenize(query)
        
        # Check for hashtag match
        hashtag_match = self._find_by_hashtag(query)
        if hashtag_match:
            return [hashtag_match]
        
        # Check for marketing code match
        marketing_match = self._find_by_marketing_code(query)
        if marketing_match:
            return [marketing_match]
        
        # Calcular scores
        results = []
        for template in self._templates:
            if template.get("status") != "active":
                continue
                
            score, reasons = self._calculate_score(tokens, query, template)
            
            if score >= min_score:
                results.append(TemplateMatch(
                    template_id=template.get("template_id", template.get("id", "")),
                    name=template["name"],
                    description_short=template.get("description_short", template.get("description", "")),
                    score=score,
                    match_reasons=reasons,
                    colors=template.get("colors", {}),
                    categories=template.get("categories", []),
                    mood=template.get("mood", []),
                    thumbnail_url=template.get("thumbnail_url", ""),
                    preview_image_url=template.get("preview_image_url", ""),
                    video_url=template.get("video_url", "")
                ))
        
        # Ordenar por score
        results.sort(key=lambda x: x.score, reverse=True)
        
        return results[:max_results]
    
    def _find_by_hashtag(self, query: str) -> Optional[TemplateMatch]:
        """Busca template por hashtag exata."""
        # Encontrar hashtags na query
        hashtags = re.findall(r'#\w+', query.lower())
        
        for template in self._templates:
            template_hashtag = template.get("hashtag", "").lower()
            if template_hashtag and template_hashtag in hashtags:
                return TemplateMatch(
                    template_id=template.get("template_id", template.get("id", "")),
                    name=template["name"],
                    description_short=template.get("description_short", template.get("description", "")),
                    score=1.0,
                    match_reasons=["Hashtag: match exato"],
                    colors=template.get("colors", {}),
                    categories=template.get("categories", []),
                    mood=template.get("mood", []),
                    thumbnail_url=template.get("thumbnail_url", ""),
                    preview_image_url=template.get("preview_image_url", ""),
                    video_url=template.get("video_url", "")
                )
        return None
    
    def _find_by_marketing_code(self, query: str) -> Optional[TemplateMatch]:
        """Busca template por código de marketing."""
        query_upper = query.upper()
        
        for template in self._templates:
            code = template.get("marketing_code", "").upper()
            if code and code in query_upper:
                return TemplateMatch(
                    template_id=template.get("template_id", template.get("id", "")),
                    name=template["name"],
                    description_short=template.get("description_short", template.get("description", "")),
                    score=1.0,
                    match_reasons=["Código Marketing: match exato"],
                    colors=template.get("colors", {}),
                    categories=template.get("categories", []),
                    mood=template.get("mood", []),
                    thumbnail_url=template.get("thumbnail_url", ""),
                    preview_image_url=template.get("preview_image_url", ""),
                    video_url=template.get("video_url", "")
                )
        return None
    
    def get_template(self, template_id: str) -> Optional[Dict]:
        """Retorna template completo pelo ID."""
        if not self._templates:
            return None
        
        for template in self._templates:
            tid = template.get("template_id", template.get("id", ""))
            if tid == template_id:
                return template
        
        return None
    
    def get_fallback_template(self) -> Optional[Dict]:
        """Retorna template fallback (quando nenhum match)."""
        # Tentar encontrar Minimal Clean ou o primeiro template
        for template in (self._templates or []):
            if "minimal" in template.get("name", "").lower():
                return template
        
        # Fallback para o primeiro template ativo
        for template in (self._templates or []):
            if template.get("status") == "active":
                return template
        
        return None
    
    def list_categories(self) -> List[Dict]:
        """Lista todas as categorias disponíveis."""
        return self._categories or []
    
    def list_templates_by_category(self, category_id: str) -> List[Dict]:
        """Lista templates de uma categoria específica."""
        templates = []
        for template in (self._templates or []):
            if template.get("status") == "active" and category_id in template.get("categories", []):
                templates.append(template)
        return templates
    
    def list_all_templates(self) -> List[Dict]:
        """Lista todos os templates ativos."""
        return [t for t in (self._templates or []) if t.get("status") == "active"]
    
    def _tokenize(self, text: str) -> List[str]:
        """Tokeniza texto em palavras normalizadas."""
        # Lowercase
        text = text.lower()
        
        # Remove pontuação
        text = re.sub(r'[^\w\s]', ' ', text)
        
        # Split e remove vazios
        tokens = [t.strip() for t in text.split() if t.strip()]
        
        # Remove stopwords comuns
        stopwords = {
            'um', 'uma', 'o', 'a', 'os', 'as', 'de', 'da', 'do', 'para', 'pra',
            'que', 'com', 'em', 'por', 'no', 'na', 'nos', 'nas', 'ao', 'aos',
            'quero', 'preciso', 'gostaria', 'fazer', 'criar', 'video', 'vídeo',
            'coloca', 'bota', 'põe', 'faz', 'me', 'ai', 'aí', 'la', 'lá'
        }
        tokens = [t for t in tokens if t not in stopwords]
        
        return tokens
    
    def _calculate_score(
        self, 
        tokens: List[str], 
        original_query: str,
        template: Dict
    ) -> Tuple[float, List[str]]:
        """
        Calcula score de match entre tokens e template.
        
        Returns:
            Tuple de (score, lista de razões do match)
        """
        score = 0.0
        reasons = []
        
        # 1. Match em keywords (peso: 0.35)
        keyword_score = self._keyword_match(tokens, template.get("keywords", []))
        if keyword_score > 0:
            score += keyword_score * 0.35
            reasons.append(f"Keywords: {keyword_score:.0%}")
        
        # 2. Match em categories (peso: 0.20)
        category_score = self._category_match(tokens, template.get("categories", []))
        if category_score > 0:
            score += category_score * 0.20
            reasons.append(f"Categoria: {category_score:.0%}")
        
        # 3. Match em mood (peso: 0.15)
        mood_score = self._mood_match(tokens, template.get("mood", []))
        if mood_score > 0:
            score += mood_score * 0.15
            reasons.append(f"Mood: {mood_score:.0%}")
        
        # 4. Match em best_for (peso: 0.15)
        bestfor_score = self._bestfor_match(tokens, template.get("best_for", []))
        if bestfor_score > 0:
            score += bestfor_score * 0.15
            reasons.append(f"Best for: {bestfor_score:.0%}")
        
        # 5. Match em example_prompts (peso: 0.15)
        example_score = self._example_match(original_query.lower(), template.get("example_prompts", []))
        if example_score > 0:
            score += example_score * 0.15
            reasons.append(f"Exemplos: {example_score:.0%}")
        
        # 6. Penalidade por NOT_for
        not_for_penalty = self._notfor_penalty(tokens, template.get("NOT_for", []))
        if not_for_penalty > 0:
            score -= not_for_penalty * 0.3
            reasons.append(f"⚠️ NOT_for: -{not_for_penalty:.0%}")
        
        # Normalizar score entre 0 e 1
        score = max(0.0, min(1.0, score))
        
        return score, reasons
    
    def _keyword_match(self, tokens: List[str], keywords: List[str]) -> float:
        """Calcula match entre tokens e keywords."""
        if not keywords:
            return 0.0
        
        keywords_lower = [k.lower() for k in keywords]
        matches = 0
        
        for token in tokens:
            for keyword in keywords_lower:
                if token in keyword or keyword in token:
                    matches += 1
                    break
        
        return min(1.0, matches / max(1, len(tokens)))
    
    def _category_match(self, tokens: List[str], categories: List[str]) -> float:
        """Calcula match entre tokens e categorias."""
        if not categories or not self._categories:
            return 0.0
        
        # Pegar keywords de cada categoria
        category_keywords = {}
        for cat in self._categories:
            category_keywords[cat["id"]] = [k.lower() for k in cat.get("keywords", [])]
        
        matches = 0
        for cat_id in categories:
            cat_kws = category_keywords.get(cat_id, [])
            for token in tokens:
                if any(token in kw or kw in token for kw in cat_kws):
                    matches += 1
                    break
        
        return min(1.0, matches / max(1, len(categories)))
    
    def _mood_match(self, tokens: List[str], moods: List[str]) -> float:
        """Calcula match entre tokens e moods."""
        if not moods:
            return 0.0
        
        moods_lower = [m.lower() for m in moods]
        matches = sum(1 for t in tokens if any(t in m or m in t for m in moods_lower))
        
        return min(1.0, matches / max(1, len(moods)))
    
    def _bestfor_match(self, tokens: List[str], best_for: List[str]) -> float:
        """Calcula match entre tokens e best_for."""
        if not best_for:
            return 0.0
        
        # Tokenizar cada item de best_for
        all_bestfor_tokens = []
        for item in best_for:
            all_bestfor_tokens.extend(self._tokenize(item))
        
        if not all_bestfor_tokens:
            return 0.0
        
        matches = sum(1 for t in tokens if t in all_bestfor_tokens)
        return min(1.0, matches / max(1, len(tokens)))
    
    def _example_match(self, query: str, examples: List[str]) -> float:
        """Calcula similaridade com example_prompts."""
        if not examples:
            return 0.0
        
        best_score = 0.0
        query_tokens = set(self._tokenize(query))
        
        for example in examples:
            example_tokens = set(self._tokenize(example.lower()))
            if not example_tokens:
                continue
            
            # Jaccard similarity
            intersection = len(query_tokens & example_tokens)
            union = len(query_tokens | example_tokens)
            
            if union > 0:
                similarity = intersection / union
                best_score = max(best_score, similarity)
        
        return best_score
    
    def _notfor_penalty(self, tokens: List[str], not_for: List[str]) -> float:
        """Calcula penalidade por match com NOT_for."""
        if not not_for:
            return 0.0
        
        # Tokenizar cada item de NOT_for
        all_notfor_tokens = []
        for item in not_for:
            all_notfor_tokens.extend(self._tokenize(item))
        
        if not all_notfor_tokens:
            return 0.0
        
        matches = sum(1 for t in tokens if t in all_notfor_tokens)
        return min(1.0, matches / max(1, len(tokens)))


# Singleton instance
_service_instance = None

def get_template_search_service() -> TemplateSearchService:
    """Retorna instância singleton do serviço."""
    global _service_instance
    if _service_instance is None:
        _service_instance = TemplateSearchService()
    return _service_instance


# Funções de conveniência
def search_templates(query: str, max_results: int = 5) -> List[TemplateMatch]:
    """Busca templates pelo query."""
    return get_template_search_service().search(query, max_results)


def get_template_by_id(template_id: str) -> Optional[Dict]:
    """Retorna template pelo ID."""
    return get_template_search_service().get_template(template_id)


def reload_templates():
    """Força recarga dos templates do banco."""
    get_template_search_service().reload_templates()


# Para uso direto como script
if __name__ == "__main__":
    # Teste básico
    service = TemplateSearchService()
    
    test_queries = [
        "quero um vídeo profissional para linkedin",
        "algo divertido e colorido pro tiktok",
        "estilo cyberpunk futurista",
        "cores suaves e delicadas",
        "visual de jornal de TV",
        "#boldimpact",
        "MINIMAL01",
        "eita coloca legendas aí pra mim"
    ]
    
    print("\n🔍 Template Search Service - Testes\n")
    print("=" * 60)
    
    for query in test_queries:
        print(f"\n📝 Query: \"{query}\"")
        results = service.search(query, max_results=3)
        
        if results:
            for i, match in enumerate(results, 1):
                print(f"   {i}. {match.name} (score: {match.score:.2f})")
                print(f"      Razões: {', '.join(match.match_reasons)}")
        else:
            print("   ❌ Nenhum match encontrado")
        
        print("-" * 60)
