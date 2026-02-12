"""
🆕 v2.9.183: Queue Router Service
Serviço para rotear jobs para workers baseado em regras configuráveis.

Avalia regras de roteamento ordenadas por prioridade e retorna o worker
mais adequado para processar o job.

Regras suportadas:
- video_duration_lt/gt: Duração do vídeo em segundos
- segments_lt/lte/gt: Número de segmentos
- queue_size_lt/gt: Tamanho da fila de um worker
- hour_between: Horário do dia
- template_id_eq/in: Template específico
"""

import os
import time
import logging
import redis
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple
import psycopg2
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


class QueueRouterService:
    """
    Roteador de filas inteligente.
    Avalia regras de roteamento e retorna o worker mais adequado.
    """
    
    def __init__(self):
        self.db_url = os.environ.get('DATABASE_URL')
        self.redis_url = os.environ.get('REDIS_URL', 'redis://redis:6379/0')
        self._rules_cache = None
        self._rules_cache_time = 0
        self._cache_ttl = 60  # Recarregar regras a cada 60 segundos
        
        logger.info("🚦 [QueueRouter] Serviço inicializado")
    
    def _get_db_connection(self):
        """Obtém conexão com o banco de dados."""
        if not self.db_url:
            raise ValueError("DATABASE_URL não configurada")
        return psycopg2.connect(self.db_url)
    
    def _get_redis_connection(self):
        """Obtém conexão com Redis."""
        return redis.from_url(self.redis_url)
    
    def _load_rules(self, applies_to: str = 'matting') -> List[Dict]:
        """Carrega regras do banco de dados (com cache)."""
        now = time.time()
        
        # Usar cache se ainda válido
        if self._rules_cache and (now - self._rules_cache_time) < self._cache_ttl:
            return [r for r in self._rules_cache if r['applies_to'] in (applies_to, 'all')]
        
        try:
            conn = self._get_db_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT * FROM routing_rules 
                    WHERE enabled = true 
                    ORDER BY priority DESC
                """)
                rules = cur.fetchall()
            conn.close()
            
            self._rules_cache = [dict(r) for r in rules]
            self._rules_cache_time = now
            
            logger.debug(f"🚦 [QueueRouter] {len(rules)} regras carregadas")
            return [r for r in self._rules_cache if r['applies_to'] in (applies_to, 'all')]
            
        except Exception as e:
            logger.error(f"🚦 [QueueRouter] Erro ao carregar regras: {e}")
            return []
    
    def _load_workers(self) -> Dict[str, Dict]:
        """Carrega workers do banco de dados."""
        try:
            conn = self._get_db_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT * FROM workers 
                    WHERE enabled = true
                """)
                workers = cur.fetchall()
            conn.close()
            
            return {w['worker_id']: dict(w) for w in workers}
            
        except Exception as e:
            logger.error(f"🚦 [QueueRouter] Erro ao carregar workers: {e}")
            return {}
    
    def _get_queue_sizes(self) -> Dict[str, int]:
        """Obtém tamanho das filas Redis."""
        try:
            r = self._get_redis_connection()
            sizes = {}
            
            # Padrões de filas conhecidas (Modal workers)
            for worker_id in ['modal', 'modal-cpu-light']:
                queue_key = f"video:queue:{worker_id}"
                sizes[worker_id] = r.llen(queue_key)
            
            return sizes
            
        except Exception as e:
            logger.warning(f"🚦 [QueueRouter] Erro ao buscar filas Redis: {e}")
            return {}
    
    def _evaluate_condition(
        self,
        condition_key: str,
        condition_value: Any,
        context: Dict[str, Any],
        queue_sizes: Dict[str, int]
    ) -> Tuple[bool, str]:
        """
        Avalia uma condição individual.
        
        Returns:
            Tuple (match: bool, reason: str)
        """
        video_duration = context.get('video_duration', 0)
        segments = context.get('segments', 1)
        template_id = context.get('template_id')
        
        # Duração do vídeo
        if condition_key == 'video_duration_lt':
            if video_duration < condition_value:
                return True, f"duration {video_duration:.1f}s < {condition_value}s"
            return False, ""
        
        if condition_key == 'video_duration_gt':
            if video_duration > condition_value:
                return True, f"duration {video_duration:.1f}s > {condition_value}s"
            return False, ""
        
        if condition_key == 'video_duration_between':
            min_d, max_d = condition_value
            if min_d <= video_duration <= max_d:
                return True, f"duration {video_duration:.1f}s in [{min_d}, {max_d}]"
            return False, ""
        
        # Segmentos
        if condition_key == 'segments_lt':
            if segments < condition_value:
                return True, f"segments {segments} < {condition_value}"
            return False, ""
        
        if condition_key == 'segments_lte':
            if segments <= condition_value:
                return True, f"segments {segments} <= {condition_value}"
            return False, ""
        
        if condition_key == 'segments_gt':
            if segments > condition_value:
                return True, f"segments {segments} > {condition_value}"
            return False, ""
        
        if condition_key == 'segments_between':
            min_s, max_s = condition_value
            if min_s <= segments <= max_s:
                return True, f"segments {segments} in [{min_s}, {max_s}]"
            return False, ""
        
        # Fila
        if condition_key == 'queue_size_gt':
            # Verificar qual worker está sendo verificado
            worker = context.get('_conditions', {}).get('worker', context.get('target_worker_id', ''))
            queue_size = queue_sizes.get(worker, 0)
            if queue_size > condition_value:
                return True, f"queue({worker}) {queue_size} > {condition_value}"
            return False, ""
        
        if condition_key == 'queue_size_lt':
            worker = context.get('_conditions', {}).get('worker', context.get('target_worker_id', ''))
            queue_size = queue_sizes.get(worker, 0)
            if queue_size < condition_value:
                return True, f"queue({worker}) {queue_size} < {condition_value}"
            return False, ""
        
        # Horário
        if condition_key == 'hour_between':
            current_hour = datetime.now().hour
            min_h, max_h = condition_value
            if min_h <= current_hour <= max_h:
                return True, f"hour {current_hour} in [{min_h}, {max_h}]"
            return False, ""
        
        # Template
        if condition_key == 'template_id_eq':
            if template_id == condition_value:
                return True, "template matches"
            return False, ""
        
        if condition_key == 'template_id_in':
            if template_id in condition_value:
                return True, "template in list"
            return False, ""
        
        # Ignorar campos especiais
        if condition_key in ('worker',):
            return True, ""  # Campo auxiliar, não é condição
        
        logger.warning(f"🚦 [QueueRouter] Condição desconhecida: {condition_key}")
        return True, ""  # Ignorar condições desconhecidas (não bloquear)
    
    def _evaluate_rule(
        self,
        rule: Dict,
        context: Dict[str, Any],
        queue_sizes: Dict[str, int]
    ) -> Tuple[bool, List[str]]:
        """
        Avalia se uma regra faz match com o contexto.
        
        Returns:
            Tuple (match: bool, reasons: List[str])
        """
        conditions = rule.get('conditions', {})
        
        # Regra sem condições = fallback (sempre match)
        if not conditions:
            return True, ["fallback (no conditions)"]
        
        # Adicionar condições ao contexto para acesso em avaliação
        context['_conditions'] = conditions
        context['target_worker_id'] = rule.get('target_worker_id', '')
        
        match_reasons = []
        
        for key, value in conditions.items():
            match, reason = self._evaluate_condition(key, value, context, queue_sizes)
            if not match:
                return False, []  # AND: qualquer falha invalida a regra
            if reason:
                match_reasons.append(reason)
        
        return True, match_reasons
    
    def _log_routing_decision(
        self,
        job_id: str,
        rule: Optional[Dict],
        selected_worker: str,
        was_fallback: bool,
        context: Dict[str, Any],
        rules_evaluated: int,
        evaluation_time_ms: int
    ):
        """Loga a decisão de roteamento para auditoria."""
        try:
            conn = self._get_db_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO routing_logs (
                        job_id, rule_id, rule_name, 
                        video_duration_seconds, segments_count, template_id,
                        selected_worker_id, was_fallback,
                        evaluation_time_ms, rules_evaluated
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    job_id if job_id and len(job_id) == 36 else None,  # Apenas UUIDs válidos
                    rule['id'] if rule else None,
                    rule['name'] if rule else None,
                    context.get('video_duration'),
                    context.get('segments'),
                    context.get('template_id') if context.get('template_id') and len(context.get('template_id', '')) == 36 else None,
                    selected_worker,
                    was_fallback,
                    evaluation_time_ms,
                    rules_evaluated
                ))
                
                # Incrementar contador de matches da regra
                if rule:
                    cur.execute("""
                        UPDATE routing_rules 
                        SET times_matched = times_matched + 1, 
                            last_matched_at = NOW()
                        WHERE id = %s
                    """, (rule['id'],))
                
                conn.commit()
            conn.close()
            
        except Exception as e:
            logger.warning(f"🚦 [QueueRouter] Erro ao logar decisão: {e}")
    
    def route(
        self,
        job_id: str,
        video_duration: float = 0,
        segments: int = 1,
        template_id: str = None,
        applies_to: str = 'matting',
        worker_override: str = None
    ) -> Dict[str, Any]:
        """
        Roteia um job para o worker mais adequado.
        
        Args:
            job_id: ID do job
            video_duration: Duração total do vídeo em segundos
            segments: Número de segmentos para processar
            template_id: ID do template (para regras específicas)
            applies_to: Tipo de serviço ('matting', 'render', 'phase1')
            worker_override: Se fornecido, força o uso deste worker (bypass das regras)
        
        Returns:
            dict com:
            - worker_id: ID do worker selecionado
            - rule_name: Nome da regra que fez match
            - rule_id: ID da regra
            - was_fallback: Se usou fallback
            - reasons: Lista de razões do match
        """
        start_time = time.time()
        
        context = {
            'video_duration': video_duration,
            'segments': segments,
            'template_id': template_id
        }
        
        logger.info(f"🚦 [QueueRouter] Roteando job {job_id[:8] if job_id else 'N/A'}...")
        logger.info(f"   Contexto: duration={video_duration:.1f}s, segments={segments}, service={applies_to}")
        logger.info(f"   🎯 worker_override recebido: {worker_override}")  # 🔧 v2.9.186: Debug log
        
        # 🆕 v2.9.184: Worker Override - bypass das regras
        if worker_override:
            workers = self._load_workers()
            override_worker = workers.get(worker_override)
            
            if override_worker and override_worker['status'] == 'online':
                logger.info(f"   🎯 [OVERRIDE] Worker forçado: {worker_override} (bypass das regras)")
                
                # Logar decisão com override
                try:
                    self._log_routing_decision(
                        job_id=job_id,
                        applies_to=applies_to,
                        input_data=context,
                        evaluated_rules=[],
                        selected_rule_id=None,
                        selected_worker_id=worker_override,
                        was_fallback=False
                    )
                except Exception as e:
                    logger.warning(f"🚦 [QueueRouter] Erro ao logar override: {e}")
                
                return {
                    'worker_id': worker_override,
                    'rule_name': f'[OVERRIDE] {worker_override}',
                    'rule_id': None,
                    'was_fallback': False,
                    'reasons': [f'worker_override={worker_override}']
                }
            else:
                logger.warning(f"   ⚠️ [OVERRIDE] Worker '{worker_override}' offline ou não existe, usando regras normais")
        
        # Carregar dados
        rules = self._load_rules(applies_to)
        workers = self._load_workers()
        queue_sizes = self._get_queue_sizes()
        
        if not rules:
            logger.warning("🚦 [QueueRouter] Nenhuma regra encontrada, usando default 'modal'")
            return {
                'worker_id': 'modal',
                'rule_name': None,
                'rule_id': None,
                'was_fallback': False,
                'reasons': ['no rules configured']
            }
        
        # Avaliar cada regra por ordem de prioridade
        selected_rule = None
        selected_worker = None
        was_fallback = False
        match_reasons = []
        
        for rule in rules:
            match, reasons = self._evaluate_rule(rule, context.copy(), queue_sizes)
            
            if match:
                selected_rule = rule
                match_reasons = reasons
                
                # Verificar se o target está disponível
                target = rule['target_worker_id']
                target_worker = workers.get(target)
                
                if target_worker and target_worker['status'] == 'online':
                    selected_worker = target
                    logger.info(f"   ✅ Regra '{rule['name']}' → {target}")
                elif rule.get('fallback_worker_id'):
                    fallback_id = rule['fallback_worker_id']
                    fallback_worker = workers.get(fallback_id)
                    if fallback_worker and fallback_worker['status'] == 'online':
                        selected_worker = fallback_id
                        was_fallback = True
                        logger.info(f"   ⚠️ Regra '{rule['name']}' → {target} offline, usando fallback {fallback_id}")
                    else:
                        selected_worker = target  # Tentar mesmo assim
                        logger.warning(f"   ⚠️ Target e fallback offline, tentando {target} mesmo assim")
                else:
                    selected_worker = target
                    
                break  # Primeira regra que faz match
        
        # Fallback absoluto se nenhuma regra fez match
        if not selected_worker:
            selected_worker = 'modal'
            logger.warning("🚦 [QueueRouter] Nenhuma regra fez match, usando default 'modal'")
        
        # Calcular tempo de avaliação
        evaluation_time_ms = int((time.time() - start_time) * 1000)
        
        # Logar decisão
        self._log_routing_decision(
            job_id=job_id,
            rule=selected_rule,
            selected_worker=selected_worker,
            was_fallback=was_fallback,
            context=context,
            rules_evaluated=len(rules),
            evaluation_time_ms=evaluation_time_ms
        )
        
        logger.info(f"🚦 [QueueRouter] Decisão: {selected_worker} (em {evaluation_time_ms}ms)")
        
        return {
            'worker_id': selected_worker,
            'rule_name': selected_rule['name'] if selected_rule else None,
            'rule_id': str(selected_rule['id']) if selected_rule else None,
            'was_fallback': was_fallback,
            'reasons': match_reasons
        }


# Singleton
_router_instance = None

def get_queue_router() -> QueueRouterService:
    """Retorna a instância singleton do router."""
    global _router_instance
    if _router_instance is None:
        _router_instance = QueueRouterService()
    return _router_instance
