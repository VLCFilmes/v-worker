#!/usr/bin/env python3
"""
🔧 Video Orchestrator Worker

Worker que consome jobs da fila Redis e executa o pipeline de processamento.
Deve ser executado na Linux Home e nas EC2 Spots.

Uso:
    python worker.py [--concurrency N] [--queue QUEUE_NAME]
    
Exemplo:
    python worker.py --concurrency 2  # Linux Home com 2 workers paralelos
    python worker.py --concurrency 4  # EC2 Spot com 4 workers paralelos
"""

import os
import sys
import time
import signal
import logging
import argparse
import threading
from datetime import datetime

# Adicionar diretório app ao path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    stream=sys.stdout
)
logger = logging.getLogger('worker')

# Reduzir verbosidade de bibliotecas
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('requests').setLevel(logging.WARNING)


class VideoWorker:
    """
    Worker que processa jobs da fila Redis.
    
    Pode executar múltiplos jobs em paralelo (até max_concurrency).
    
    🆕 v2.9.95: Suporte a WORKER_TYPE para roteamento de jobs:
    - 'hetzner': processa jobs com worker_preference='hetzner' ou 'auto'
    - 'linux-home': processa jobs com worker_preference='home_only' ou 'auto'
    """
    
    def __init__(self, queue_name: str = 'video_orchestrator', max_concurrency: int = 2):
        self.queue_name = queue_name
        self.max_concurrency = max_concurrency
        self.running = True
        self.active_jobs = 0
        self.active_jobs_lock = threading.Lock()
        self.processed_count = 0
        self.failed_count = 0
        
        # 🆕 v2.9.95: Identificação do worker
        self.worker_type = os.environ.get('WORKER_TYPE', 'hetzner')  # hetzner ou linux-home
        logger.info(f"🏷️ Worker type: {self.worker_type}")
        
        # Conectar ao Redis
        self.redis_host = os.environ.get('REDIS_HOST', 'localhost')
        self.redis_port = int(os.environ.get('REDIS_PORT', 6379))
        self.redis_password = os.environ.get('REDIS_PASSWORD', None)
        self._connect_redis()
        
        # Registrar worker
        self._register_worker()
        
        # Configurar signal handlers
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)
    
    def _connect_redis(self):
        """Conecta ao Redis"""
        try:
            from redis import Redis
            self.redis = Redis(
                host=self.redis_host,
                port=self.redis_port,
                password=self.redis_password,
                decode_responses=True
            )
            self.redis.ping()
            auth_status = "🔒 (com senha)" if self.redis_password else "⚠️ (sem senha)"
            logger.info(f"✅ Conectado ao Redis: {self.redis_host}:{self.redis_port} {auth_status}")
        except Exception as e:
            logger.error(f"❌ Falha ao conectar ao Redis: {e}")
            sys.exit(1)
    
    def _get_queue_size(self) -> int:
        """Retorna o tamanho atual da fila"""
        try:
            return self.redis.llen(self.queue_name) or 0
        except:
            return 0
    
    def _get_ec2_spot_ip(self) -> str:
        """Retorna IP do EC2 Spot se disponível, None se não"""
        try:
            ec2_ip = self.redis.get('vinicius:spot:current_ip')
            if ec2_ip:
                # Health check rápido
                import requests
                try:
                    response = requests.get(f"http://{ec2_ip}:5000/health", timeout=3)
                    if response.status_code == 200:
                        return ec2_ip
                except:
                    pass
            return None
        except:
            return None
    
    def _should_use_ec2_spot(self) -> tuple:
        """
        Decide se deve usar EC2 Spot baseado em:
        - Tamanho da fila
        - Disponibilidade do EC2 Spot
        
        Retorna: (usar_ec2: bool, ec2_ip: str ou None)
        """
        queue_size = self._get_queue_size()
        ec2_ip = self._get_ec2_spot_ip()
        
        # Se fila >= 2 E EC2 Spot disponível → usar EC2
        if queue_size >= 2 and ec2_ip:
            logger.info(f"🔀 Fila com {queue_size} jobs, usando EC2 Spot: {ec2_ip}")
            return True, ec2_ip
        
        # Caso contrário → usar Linux Home (padrão)
        return False, None
    
    def _get_job_worker_preference(self, job_id: str) -> str:
        """
        🆕 v2.9.117: Busca worker_preference do job no banco de dados.
        
        Returns:
            'ec2_unified', 'home_only', 'auto', ou outro valor configurado
        """
        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor
            
            db_url = os.environ.get('DB_REMOTE_URL')
            if not db_url:
                logger.warning("⚠️ DB_REMOTE_URL não configurado, usando 'auto'")
                return 'auto'
            
            conn = psycopg2.connect(db_url)
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            cursor.execute("""
                SELECT options->>'worker_preference' as worker_preference
                FROM video_processing_jobs 
                WHERE job_id = %s
            """, (job_id,))
            
            row = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if row and row.get('worker_preference'):
                return row['worker_preference']
            
            return 'auto'
            
        except Exception as e:
            logger.warning(f"⚠️ Erro ao buscar worker_preference: {e}")
            return 'auto'
    
    def _should_this_worker_process(self, job_id: str, worker_preference: str) -> bool:
        """
        🆕 v2.9.95: Verifica se este worker deve processar o job.
        
        Lógica:
        - 'hetzner' → apenas worker type 'hetzner' processa
        - 'home_only' → apenas worker type 'linux-home' processa
        - 'auto' → qualquer worker pode processar (preferência: hetzner)
        
        Returns:
            True se este worker deve processar, False se deve devolver
        """
        my_type = self.worker_type
        
        if worker_preference == 'hetzner':
            # Job explicitamente pediu Hetzner
            if my_type == 'hetzner':
                logger.info(f"🇩🇪 Job {job_id[:8]}... solicita Hetzner → processando")
                return True
            else:
                logger.debug(f"🇩🇪 Job {job_id[:8]}... solicita Hetzner (sou {my_type})")
                return False
                
        elif worker_preference == 'home_only':
            # Job explicitamente pediu Linux Home
            if my_type == 'linux-home':
                logger.info(f"🏠 Job {job_id[:8]}... solicita Linux Home → processando")
                return True
            else:
                logger.debug(f"🏠 Job {job_id[:8]}... solicita Linux Home (sou {my_type})")
                return False
                
        else:
            # 'auto' ou outro → preferência para Hetzner, mas Linux Home pode processar se fila grande
            if my_type == 'hetzner':
                logger.info(f"⚡ Job {job_id[:8]}... modo auto → Hetzner processa")
                return True
            else:
                # Linux Home só processa auto se fila tiver mais de 2 jobs
                queue_size = self._get_queue_size()
                if queue_size >= 2:
                    logger.info(f"⚡ Job {job_id[:8]}... modo auto + fila={queue_size} → Linux Home processa")
                    return True
                else:
                    # Devolver para Hetzner processar
                    logger.debug(f"⚡ Job {job_id[:8]}... modo auto + fila={queue_size} → deixar para Hetzner")
                    return False
    
    def _register_worker(self):
        """Registra worker no Redis para monitoramento"""
        import socket
        hostname = socket.gethostname()
        worker_id = f"worker:{hostname}:{os.getpid()}"
        
        self.worker_id = worker_id
        self.redis.hset(f"worker:{worker_id}", mapping={
            'hostname': hostname,
            'pid': os.getpid(),
            'queue': self.queue_name,
            'max_concurrency': self.max_concurrency,
            'started_at': datetime.now().isoformat(),
            'status': 'running'
        })
        self.redis.expire(f"worker:{worker_id}", 300)  # TTL 5 min, renovado no heartbeat
        
        logger.info(f"📝 Worker registrado: {worker_id}")
    
    def _handle_shutdown(self, signum, frame):
        """Graceful shutdown"""
        logger.info(f"🛑 Recebido sinal {signum}, finalizando...")
        self.running = False
    
    def _heartbeat(self):
        """Envia heartbeat ao Redis periodicamente"""
        while self.running:
            try:
                self.redis.hset(f"worker:{self.worker_id}", mapping={
                    'last_heartbeat': datetime.now().isoformat(),
                    'active_jobs': self.active_jobs,
                    'processed': self.processed_count,
                    'failed': self.failed_count,
                    'status': 'running'
                })
                self.redis.expire(f"worker:{self.worker_id}", 300)
            except Exception as e:
                logger.warning(f"⚠️ Erro no heartbeat: {e}")
            
            time.sleep(30)
    
    def _process_job(self, job_id: str, ec2_ip: str = None):
        """
        Processa um job específico (Fase 1 → execute_pipeline).
        
        Args:
            job_id: ID do job a processar
            ec2_ip: IP do EC2 Spot (reservado para uso futuro)
        """
        logger.info(f"🎬 Processando job: {job_id}")
        
        start_time = time.time()
        
        try:
            from app.video_orchestrator.engine.bridge import get_engine_bridge
            bridge = get_engine_bridge()
            bridge.execute_pipeline(job_id)
            
            elapsed = time.time() - start_time
            logger.info(f"✅ Job {job_id} concluído em {elapsed:.1f}s")
            self.processed_count += 1
            self._log_job_result(job_id, 'completed', elapsed)
            
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"❌ Job {job_id} falhou após {elapsed:.1f}s: {e}")
            self.failed_count += 1
            self._log_job_result(job_id, 'failed', elapsed, str(e))
            
            # Bridge já marca o job como FAILED internamente
        
        finally:
            with self.active_jobs_lock:
                self.active_jobs -= 1
    
    def _process_continue_job(self, job_id: str):
        """
        Processa um job de continue_pipeline (Fase 2).
        
        O bridge lê overrides (template_id, worker) do job.options['_continue_params']
        e decide quais steps rodar (PHASE_2_STEPS). O worker apenas despacha.
        
        Args:
            job_id: ID do novo job (Fase 2) a processar
        """
        logger.info(f"🔄 [CONTINUE] Processando continue_pipeline: {job_id}")
        
        start_time = time.time()
        
        try:
            from app.video_orchestrator.engine.bridge import get_engine_bridge
            bridge = get_engine_bridge()
            bridge.continue_pipeline(job_id)
            
            elapsed = time.time() - start_time
            logger.info(f"✅ [CONTINUE] Job {job_id} concluído em {elapsed:.1f}s")
            self.processed_count += 1
            self._log_job_result(job_id, 'completed', elapsed)
            
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"❌ [CONTINUE] Job {job_id} falhou após {elapsed:.1f}s: {e}")
            self.failed_count += 1
            self._log_job_result(job_id, 'failed', elapsed, str(e))
            
            # Bridge já marca o job como FAILED internamente
        
        finally:
            with self.active_jobs_lock:
                self.active_jobs -= 1
    
    def _process_replay_job(self, job_id: str):
        """
        🆕 v3.10.0: Processa um job de replay_pipeline (Pipeline Replay).
        
        O bridge lê o state reconstruído (checkpoint + modifications) e os
        steps a executar do job.options['_replay_params']. O worker apenas despacha.
        
        Args:
            job_id: ID do novo job (criado pelo endpoint replay-from)
        """
        logger.info(f"🔄 [REPLAY] Processando replay_pipeline: {job_id}")
        
        start_time = time.time()
        
        try:
            from app.video_orchestrator.engine.bridge import get_engine_bridge
            bridge = get_engine_bridge()
            bridge.replay_pipeline(job_id)
            
            elapsed = time.time() - start_time
            logger.info(f"✅ [REPLAY] Job {job_id} concluído em {elapsed:.1f}s")
            self.processed_count += 1
            self._log_job_result(job_id, 'completed', elapsed)
            
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"❌ [REPLAY] Job {job_id} falhou após {elapsed:.1f}s: {e}")
            self.failed_count += 1
            self._log_job_result(job_id, 'failed', elapsed, str(e))
        
        finally:
            with self.active_jobs_lock:
                self.active_jobs -= 1
    
    def _log_job_result(self, job_id: str, status: str, duration: float, error: str = None):
        """Registra resultado do job no Redis para monitoramento"""
        try:
            import json
            result = {
                'job_id': job_id,
                'status': status,
                'duration_seconds': round(duration, 1),
                'timestamp': datetime.now().isoformat(),
                'worker_id': self.worker_id
            }
            if error:
                result['error'] = error[:500]  # Limitar tamanho do erro
            
            # Adicionar a uma lista de resultados recentes
            self.redis.lpush('vinicius:worker:results', json.dumps(result))
            self.redis.ltrim('vinicius:worker:results', 0, 99)  # Manter só últimos 100
            
            # Sincronizar com spot_test_results se este job veio de um test run
            self._sync_test_result(job_id, status, duration, error)
            
        except Exception as e:
            logger.warning(f"⚠️ Erro ao registrar resultado: {e}")
    
    def _sync_test_result(self, job_id: str, status: str, duration: float, error: str = None):
        """
        Sincroniza resultado do job com a tabela spot_test_results.
        Busca video_url do video_processing_jobs e atualiza spot_test_results.
        """
        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor
            
            db_url = os.environ.get('DB_REMOTE_URL')
            if not db_url:
                return
            
            conn = psycopg2.connect(db_url)
            conn.autocommit = True
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Buscar video_url do job real
            cursor.execute("""
                SELECT final_video_url 
                FROM video_processing_jobs 
                WHERE id = %s
            """, (job_id,))
            job_row = cursor.fetchone()
            video_url = job_row.get('final_video_url') if job_row else None
            
            # Atualizar spot_test_results se existir para este job_id
            cursor.execute("""
                UPDATE spot_test_results 
                SET 
                    status = %s,
                    total_duration_ms = %s,
                    video_url = %s,
                    error_message = %s,
                    completed_at = NOW(),
                    updated_at = NOW()
                WHERE job_id = %s::uuid
            """, (
                status,
                int(duration * 1000) if duration else None,
                video_url,
                error[:500] if error else None,
                job_id
            ))
            
            if cursor.rowcount > 0:
                logger.info(f"📝 Sincronizado spot_test_results para job {job_id}")
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            logger.warning(f"⚠️ Erro ao sincronizar test result: {e}")
    
    def _parse_queue_message(self, raw_message: str) -> dict:
        """
        🆕 v3.3.0 / v3.10.0: Parseia mensagem da fila Redis.
        
        Suporta formatos (retrocompatível):
        - String pura (UUID): job de _execute_pipeline (Fase 1)
        - JSON {"action": "continue_pipeline", "job_id": "xxx"}: Fase 2
        - JSON {"action": "replay_pipeline", "job_id": "xxx"}: Pipeline Replay
        
        Returns:
            dict com 'action' e 'job_id'
        """
        import json
        
        # Tentar parsear como JSON
        if raw_message.startswith('{'):
            try:
                data = json.loads(raw_message)
                return {
                    'action': data.get('action', 'execute_pipeline'),
                    'job_id': data.get('job_id', raw_message)
                }
            except (json.JSONDecodeError, KeyError):
                pass
        
        # Fallback: string pura = job_id para _execute_pipeline
        return {
            'action': 'execute_pipeline',
            'job_id': raw_message
        }
    
    def run(self):
        """Loop principal do worker"""
        logger.info(f"🚀 Worker iniciado: queue={self.queue_name}, concurrency={self.max_concurrency}")
        logger.info(f"🆕 v3.10.0: Suporte a execute/continue/replay_pipeline via Redis")
        
        # Iniciar thread de heartbeat
        heartbeat_thread = threading.Thread(target=self._heartbeat, daemon=True)
        heartbeat_thread.start()
        
        while self.running:
            try:
                # Verificar se pode processar mais jobs
                with self.active_jobs_lock:
                    if self.active_jobs >= self.max_concurrency:
                        time.sleep(0.5)
                        continue
                
                # Tentar pegar job da fila (blocking com timeout de 5s)
                result = self.redis.blpop(self.queue_name, timeout=5)
                
                if result is None:
                    continue  # Timeout, fila vazia
                
                queue_name, raw_message = result
                
                # 🆕 v3.3.0: Parsear mensagem (suporta string pura e JSON)
                parsed = self._parse_queue_message(raw_message)
                job_id = parsed['job_id']
                action = parsed['action']
                
                logger.info(f"📬 Mensagem recebida: action={action}, job_id={job_id[:8]}...")
                
                # 🆕 v2.9.117: Verificar worker_preference do job ANTES de decidir onde processar
                job_worker_preference = self._get_job_worker_preference(job_id)
                
                # 🆕 v2.9.95: Verificar se este worker deve processar o job
                should_process = self._should_this_worker_process(job_id, job_worker_preference)
                
                if not should_process:
                    # Devolver job para o final da fila (outro worker pegará)
                    # Devolver a mensagem original (preservando formato JSON se for continue)
                    self.redis.rpush(self.queue_name, raw_message)
                    logger.info(f"↩️ Job {job_id[:8]}... devolvido para fila (não é para este worker: {self.worker_type})")
                    continue
                
                # Incrementar contador de jobs ativos
                with self.active_jobs_lock:
                    self.active_jobs += 1
                
                # 🆕 v3.3.0 / v3.10.0: Rotear para o método correto baseado na action
                if action == 'continue_pipeline':
                    logger.info(f"🔄 [CONTINUE] Roteando job {job_id[:8]}... para continue_pipeline")
                    thread = threading.Thread(
                        target=self._process_continue_job,
                        args=(job_id,),
                        daemon=True
                    )
                elif action == 'replay_pipeline':
                    logger.info(f"🔄 [REPLAY] Roteando job {job_id[:8]}... para replay_pipeline")
                    thread = threading.Thread(
                        target=self._process_replay_job,
                        args=(job_id,),
                        daemon=True
                    )
                else:
                    # Padrão: _execute_pipeline (Fase 1 completa)
                    ec2_ip = None
                    thread = threading.Thread(
                        target=self._process_job,
                        args=(job_id, ec2_ip),
                        daemon=True
                    )
                
                thread.start()
                
            except Exception as e:
                logger.error(f"❌ Erro no loop do worker: {e}")
                time.sleep(1)
        
        # Aguardar jobs ativos terminarem
        logger.info(f"⏳ Aguardando {self.active_jobs} jobs ativos terminarem...")
        while self.active_jobs > 0:
            time.sleep(0.5)
        
        # Remover registro do worker
        self.redis.delete(f"worker:{self.worker_id}")
        
        logger.info(f"👋 Worker finalizado. Processados: {self.processed_count}, Falhas: {self.failed_count}")


def main():
    parser = argparse.ArgumentParser(description='Video Orchestrator Worker')
    parser.add_argument('--concurrency', '-c', type=int, default=2,
                        help='Número máximo de jobs paralelos (default: 2)')
    parser.add_argument('--queue', '-q', type=str, default='video_orchestrator',
                        help='Nome da fila Redis (default: video_orchestrator)')
    
    args = parser.parse_args()
    
    worker = VideoWorker(
        queue_name=args.queue,
        max_concurrency=args.concurrency
    )
    worker.run()


if __name__ == '__main__':
    main()
