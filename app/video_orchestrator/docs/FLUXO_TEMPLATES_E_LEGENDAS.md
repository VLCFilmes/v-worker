# 🎬 Fluxo de Templates e Geração de Legendas

> **Data**: Dezembro 2025  
> **Status**: Documentação de Arquitetura

---

## 📋 Visão Geral

O sistema de geração de vídeos usa **templates pré-criados** por administradores humanos. A IA **não cria templates**, ela **busca e adapta** templates existentes.

---

## 🏗️ Arquitetura de Templates

### Origem dos Templates

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        GENERATOR V2 (Site Admin)                            │
│                     site_admin_vinicius.ai                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  👨‍💼 ADMIN HUMANO                                                           │
│       ↓                                                                     │
│  📝 Cria Template                                                          │
│       ├── multi-text-styling (estilos visuais)                            │
│       ├── enhanced-phrase-rules (regras de fraseamento)                   │
│       ├── phrase-classification (classificação de frases)                 │
│       ├── project-settings (resolução, fps)                               │
│       └── ...outros items                                                  │
│       ↓                                                                     │
│  💾 Salva no Banco                                                         │
│       ├── template_config (JSON completo)                                 │
│       ├── description (para LLM buscar)                                   │
│       └── keywords (para LLM buscar)                                      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    ↓
                            Templates Prontos
                                    ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│                        SITE OFICIAL (vinicius.ai.v2)                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  👤 USUÁRIO escolhe:                                                       │
│       ├── Template específico (manual)                                     │
│       └── "IA decide" (automático)                                         │
│                                                                             │
│  Se "IA decide":                                                           │
│       ↓                                                                     │
│  🤖 LLM busca template                                                     │
│       ├── Pesquisa por keywords e description                             │
│       ├── Seleciona template mais adequado                                │
│       ├── Pode ajustar cores (personalização leve)                        │
│       └── ❌ NÃO cria template novo                                        │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    ↓
                            template_config
                                    ↓
┌─────────────────────────────────────────────────────────────────────────────┐
│                        ORCHESTRATOR (custom-api)                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  Recebe template_config escolhido                                          │
│       ↓                                                                     │
│  Processa vídeo do usuário:                                                │
│       1. Transcrição                                                       │
│       2. Fraseamento (usando enhanced-phrase-rules do template)           │
│       3. Classificação (usando phrase-classification do template)         │
│       4. Geração de PNGs (usando multi-text-styling do template)          │
│       5. Renderização final (Remotion)                                    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    ↓
                        Vídeo Final com Legendas
```

---

## 📦 Estrutura de um Template

### Campos Principais

```json
{
  "id": "template-uuid",
  "name": "Template Impactante",
  "description": "Template para vídeos motivacionais com texto grande e cores vibrantes",
  "keywords": ["motivacional", "impacto", "energia", "cores vibrantes"],
  "category": "motivation",
  "is_public": true,
  "created_by": "admin-user-id",
  
  "template_config": {
    "project-settings": { ... },
    "multi-text-styling": {
      "text_styles": {
        "default": { ... },
        "emphasis": { ... },
        "letter_effect": { ... }
      }
    },
    "enhanced-phrase-rules": { ... },
    "phrase-classification": { ... },
    "multi-backgrounds": { ... }
  }
}
```

### Para Busca pela LLM

| Campo | Uso |
|-------|-----|
| `name` | Nome amigável do template |
| `description` | Descrição detalhada para matching semântico |
| `keywords` | Tags para busca rápida |
| `category` | Categoria (motivation, tutorial, storytelling, etc.) |

---

## 🔄 Fluxo de Seleção de Template

### Opção 1: Usuário Escolhe Manualmente

```
Usuário → Lista de Templates → Seleciona → template_id → Orchestrator
```

### Opção 2: "IA Decide"

```
Usuário → "IA decide" → LLM analisa contexto do vídeo
                              ↓
                        Busca templates por:
                        - keywords matching
                        - description similarity
                        - category relevance
                              ↓
                        Seleciona melhor match
                              ↓
                        (Opcional) Ajusta cores
                              ↓
                        template_id + overrides → Orchestrator
```

---

## 🎨 Personalização pela LLM

A LLM **pode** fazer ajustes leves:

| Permitido | Não Permitido |
|-----------|---------------|
| ✅ Mudar cores do texto | ❌ Criar novas fontes |
| ✅ Ajustar cores de destaque | ❌ Alterar estrutura de bordas |
| ✅ Trocar cores de fundo | ❌ Modificar regras de fraseamento |

### Exemplo de Override

```json
{
  "template_id": "uuid-do-template",
  "overrides": {
    "multi-text-styling.text_styles.default.render_config.solid_color_rgb": "255,200,0",
    "multi-text-styling.text_styles.emphasis.render_config.solid_color_rgb": "255,100,0"
  }
}
```

---

## 📝 Responsabilidades

| Componente | Responsabilidade |
|------------|------------------|
| **Generator V2** | Criar e editar templates (humanos) |
| **Site Admin** | Hospedar o Generator V2 |
| **Site Oficial** | Interface do usuário final |
| **LLM** | Buscar e selecionar templates, ajustes leves |
| **Orchestrator** | Processar vídeo usando template |
| **V-Services** | Gerar PNGs das legendas |
| **Remotion** | Renderizar vídeo final |

---

## 🚀 Próximos Passos

1. [ ] Implementar busca de templates por keywords
2. [ ] Criar endpoint de seleção por LLM
3. [ ] Sistema de overrides para cores
4. [ ] Dashboard de templates para admin

---

**Autor**: Claude (Assistente AI)  
**Versão**: 1.0

