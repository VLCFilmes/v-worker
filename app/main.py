"""
v-worker API - Pipeline de Video e Orquestracao (separado do v-api)

Contem:
- Video Orchestrator (pipeline de processamento)
- Template Management (CRUD, sidecars, items)
- Asset Collections
- Pipeline Admin (observabilidade)
- Queue Admin (workers, filas)
- Render Versions (historico)
- SSE Stream (pipeline visualizer)
- Debug (re-render, payload inspection)

Comunicacao:
- Redis: filas de jobs (compartilhado com v-api)
- HTTP: callbacks para v-api (webhook)
- PostgreSQL: banco compartilhado
"""

from flask import Flask
from .db import init_db_pool, close_db_pool, get_pool_stats

# Video routes
from .routes.templates import templates_bp
from .routes.video_parameters import video_parameters_bp
from .routes.video_templates import video_templates_bp
from .routes.renders import renders_bp
from .routes.asset_collections import asset_collections_bp
from .routes.template_master import template_master_bp
from .routes.sidecars import sidecars_bp
from .routes.template_master_items import items_bp
from .routes.template_sidecars_modular import sidecars_bp as sidecars_modular_bp
from .routes.pipeline_admin import pipeline_admin_bp
from .routes.render_versions import render_versions_bp
from .routes.queue_admin import queue_admin_bp
from .routes.sse_stream import sse_bp
from .routes.debug import debug_bp, director_payload_bp

# Video Orchestrator
from .video_orchestrator import video_orchestrator_bp
from .video_orchestrator.callbacks import video_callbacks_bp


def create_app():
    """Cria e configura a aplicacao Flask do v-worker."""
    app = Flask(__name__)

    # === VIDEO ROUTES ===
    app.register_blueprint(templates_bp)
    app.register_blueprint(video_parameters_bp)
    app.register_blueprint(video_templates_bp, url_prefix='/api')
    app.register_blueprint(renders_bp, url_prefix='/api')
    app.register_blueprint(asset_collections_bp, url_prefix='/api')
    app.register_blueprint(template_master_bp, url_prefix='/api')
    app.register_blueprint(sidecars_bp)
    app.register_blueprint(items_bp)
    app.register_blueprint(sidecars_modular_bp)
    app.register_blueprint(pipeline_admin_bp)
    app.register_blueprint(render_versions_bp)
    app.register_blueprint(queue_admin_bp)
    app.register_blueprint(sse_bp)
    app.register_blueprint(debug_bp)
    app.register_blueprint(director_payload_bp)

    # === VIDEO ORCHESTRATOR ===
    app.register_blueprint(video_orchestrator_bp, url_prefix='/api')
    app.register_blueprint(video_callbacks_bp, url_prefix='/api')

    # === DB POOL ===
    with app.app_context():
        try:
            init_db_pool()
            print("✅ [v-worker] Connection Pool PostgreSQL inicializado")
        except Exception as e:
            print(f"⚠️ [v-worker] Erro ao inicializar Connection Pool: {e}")

    @app.route('/health')
    def health_check():
        return "v-worker API is healthy!"

    @app.route('/health/db')
    def health_check_db():
        from flask import jsonify
        stats = get_pool_stats()
        return jsonify({
            "service": "v-worker",
            "status": "healthy",
            "database_pool": stats
        })

    import atexit
    atexit.register(close_db_pool)

    return app


app = create_app()
