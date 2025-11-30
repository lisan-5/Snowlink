# 🔗 snowlink-ai

**Intelligent bi-directional sync between Atlassian (Jira + Confluence) and Snowflake using OpenAI GPT-5**

![Python](https://img.shields.io/badge/python-3.10+-blue.svg)
![OpenAI](https://img.shields.io/badge/OpenAI-GPT--5-green.svg)
![License](https://img.shields.io/badge/license-MIT-purple.svg)

---

## ✨ What it does

1. **Watches** for new/updated Confluence pages OR Jira issues (e.g., new data model tickets)
2. **Extracts** table names, column descriptions, business logic, and ownership using GPT-4o
3. **Syncs** comments to Snowflake tables and columns automatically
4. **Generates** dbt-style model documentation (`model.sql` + `schema.yml`)
5. **Creates** beautiful ER diagrams with Mermaid and posts them back to Confluence

---

## 🚀 Quick Start

### 1. Install dependencies

\`\`\`bash
pip install -r requirements.txt
\`\`\`

### 2. Configure environment

Copy `.env.example` to `.env` and fill in your credentials:

\`\`\`bash
cp .env.example .env
\`\`\`

### 3. Configure watchers

Edit `config.yaml` to specify which Jira projects and Confluence spaces to monitor.

### 4. Run the tool

\`\`\`bash
# Interactive CLI
python main.py

# Watch mode (continuous sync)
python main.py --watch

# One-time sync for a specific Confluence page
python main.py --confluence-page 12345678

# One-time sync for a Jira issue
python main.py --jira-issue PROJ-123

# Start web dashboard
python main.py --web
\`\`\`

---

## 📁 Project Structure

\`\`\`
snowlink-ai/
├── .env                  # Your secrets
├── .env.example          # Template for secrets
├── main.py               # CLI entrypoint
├── config.yaml           # Which projects/spaces to watch
├── requirements.txt      # Python dependencies
├── agent/
│   ├── __init__.py
│   ├── jira_watcher.py       # Jira polling/webhooks
│   ├── confluence_watcher.py # Confluence polling/webhooks
│   ├── snowflake_client.py   # Snowflake operations
│   ├── llm_extractor.py      # GPT-4o magic happens here
│   ├── dbt_generator.py      # Generate dbt models
│   └── er_diagram.py         # Mermaid ER diagrams
├── prompts/
│   ├── extract_schema.txt    # Schema extraction prompt
│   ├── write_comments.txt    # Comment generation prompt
│   └── dbt_schema_yml.txt    # dbt YAML generation prompt
├── output/
│   └── dbt_models/           # Generated .sql + schema.yml
└── README.md
\`\`\`

---

## 🔧 Configuration

### config.yaml

\`\`\`yaml
jira:
  projects:
    - key: DATA
      issue_types: [Story, Task, Bug]
    - key: ANALYTICS
      issue_types: [Story]

confluence:
  spaces:
    - key: DATAMODELS
    - key: ANALYTICS

snowflake:
  database: ANALYTICS
  schema: PUBLIC
  warehouse: COMPUTE_WH

sync:
  interval_seconds: 300  # Poll every 5 minutes
  batch_size: 10
\`\`\`

---

## 🧠 How the LLM Extraction Works

The core magic happens in `agent/llm_extractor.py`. It uses a carefully crafted prompt to:

1. Parse unstructured text from Confluence/Jira
2. Identify Snowflake table and column references
3. Extract business context and ownership
4. Return structured JSON for downstream processing

---

## 📊 Generated Outputs

### Snowflake Comments
\`\`\`sql
COMMENT ON TABLE DIM_CLIENT IS 'Master table containing all client demographic data';
COMMENT ON COLUMN DIM_CLIENT.CLIENT_ID IS 'Unique identifier from source system XYZ';
\`\`\`

### dbt Model (output/dbt_models/dim_client.sql)
\`\`\`sql
{{ config(materialized='table') }}

SELECT
    CLIENT_ID,
    CLIENT_NAME,
    -- ... other columns
FROM {{ source('raw', 'clients') }}
\`\`\`

### dbt Schema (output/dbt_models/schema.yml)
\`\`\`yaml
models:
  - name: dim_client
    description: Master table containing all client demographic data
    columns:
      - name: CLIENT_ID
        description: Unique identifier from source system XYZ
\`\`\`

---

## 🛠️ Advanced Features

- **Webhook Mode**: Instant sync via Jira/Confluence webhooks → FastAPI
- **ER Diagrams**: Auto-generated Mermaid diagrams posted back to Confluence
- **Slack/Teams Alerts**: Notifications when sync completes
- **Web Dashboard**: Real-time monitoring at `http://localhost:8000`

---

## 📝 License

MIT License - feel free to use and modify!
