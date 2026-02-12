"""
Módulo de conexão com o banco de dados PostgreSQL.

Arquitetura Dual (08/Fev/2026):
- get_db_cursor()    → usa Connection Pool (ThreadedConnectionPool) ✅ RECOMENDADO
- get_db_connection() → cria conexão direta (sem pool) para endpoints legados

Histórico:
- get_db_connection() originalmente usava pool, mas 85+ endpoints
  nunca devolviam a conexão → "connection pool exhausted" → HTTP 500.
- Fix: get_db_connection() agora cria conexão direta (auto-fechada pelo GC).
- Pool continua ativo para get_db_cursor() que gerencia corretamente.
- Migração gradual dos endpoints para get_db_cursor() é o plano.
"""
import logging
import os
import psycopg2
import psycopg2.extras
from psycopg2 import pool
from contextlib import contextmanager
import threading
import time

logger = logging.getLogger(__name__)

# =============================================================================
# CONNECTION POOL SINGLETON
# =============================================================================

class DatabasePool:
    """
    Singleton para gerenciar o pool de conexões PostgreSQL.
    Thread-safe e com reconexão automática.
    """
    _instance = None
    _lock = threading.Lock()
    _pool = None
    _initialized = False
    
    # Configurações do pool
    MIN_CONNECTIONS = 5    # Mínimo de conexões mantidas abertas
    MAX_CONNECTIONS = 50   # Máximo de conexões simultâneas
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def initialize(self):
        """Inicializa o pool de conexões (chamado uma vez na inicialização da app)"""
        if self._initialized:
            return
        
        with self._lock:
            if self._initialized:
                return
            
            try:
                self._pool = pool.ThreadedConnectionPool(
                    minconn=self.MIN_CONNECTIONS,
                    maxconn=self.MAX_CONNECTIONS,
                    host=os.getenv('POSTGRES_HOST'),
                    database=os.getenv('POSTGRES_DB'),
                    user=os.getenv('POSTGRES_USER'),
                    password=os.getenv('POSTGRES_PASSWORD'),
                    port=os.getenv('POSTGRES_PORT', 5432),
                    sslmode=os.getenv('POSTGRES_SSL_MODE', 'prefer'),
                    # Timeouts para evitar conexões penduradas
                    connect_timeout=10,
                    options='-c statement_timeout=30000'  # 30s timeout para queries
                )
                self._initialized = True
                logger.info(f"✅ Connection Pool inicializado: {self.MIN_CONNECTIONS}-{self.MAX_CONNECTIONS} conexões")
                logger.info(f"   Host: {os.getenv('POSTGRES_HOST')}")
                logger.info(f"   Database: {os.getenv('POSTGRES_DB')}")
            except Exception as e:
                logger.error(f"❌ Erro ao criar Connection Pool: {str(e)}")
                raise
    
    def get_connection(self):
        """Obtém uma conexão do pool"""
        if not self._initialized:
            self.initialize()
        
        try:
            conn = self._pool.getconn()
            # Verificar se a conexão ainda está válida
            if conn.closed:
                logger.warning("⚠️ Conexão estava fechada, obtendo nova...")
                self._pool.putconn(conn, close=True)
                conn = self._pool.getconn()
            return conn
        except Exception as e:
            logger.error(f"❌ Erro ao obter conexão do pool: {str(e)}")
            # Tentar reinicializar o pool
            self._initialized = False
            self.initialize()
            return self._pool.getconn()
    
    def return_connection(self, conn, close=False):
        """Devolve uma conexão ao pool"""
        if self._pool and conn:
            try:
                self._pool.putconn(conn, close=close)
            except Exception as e:
                logger.error(f"❌ Erro ao devolver conexão: {str(e)}")
    
    def close_all(self):
        """Fecha todas as conexões (para shutdown graceful)"""
        if self._pool:
            self._pool.closeall()
            self._initialized = False
            logger.info("🔌 Connection Pool fechado")
    
    def get_stats(self):
        """Retorna estatísticas do pool"""
        if not self._pool:
            return {"status": "not_initialized"}
        
        return {
            "status": "active",
            "min_connections": self.MIN_CONNECTIONS,
            "max_connections": self.MAX_CONNECTIONS,
            "initialized": self._initialized
        }


# Instância global do pool
_db_pool = DatabasePool()


# =============================================================================
# FUNÇÕES PÚBLICAS
# =============================================================================

def get_db_connection():
    """
    Retorna conexão DIRETA ao PostgreSQL (sem pool).
    
    ⚠️ ATENÇÃO: Esta função cria conexão direta para compatibilidade
    com 85+ endpoints legados que não devolvem conexão ao pool.
    A conexão é fechada pelo garbage collector ou por conn.close().
    
    Para código novo, use get_db_cursor() que usa o pool corretamente.
    
    Histórico:
        - Antes: usava pool (_db_pool.get_connection())
        - Problema: 85 endpoints pegavam conexão e NUNCA devolviam (leak)
        - Resultado: pool exhausted → 500 em tudo
        - Fix (08/Fev/2026): conexão direta para endpoints legados
    
    Returns:
        psycopg2.connection: Conexão direta (sem pool)
    """
    try:
        connection = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST'),
            database=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            port=os.getenv('POSTGRES_PORT', 5432),
            sslmode=os.getenv('POSTGRES_SSL_MODE', 'prefer'),
            connect_timeout=10,
            options='-c statement_timeout=30000',
        )
        return connection
    except Exception as e:
        logger.error(f"❌ Erro ao conectar no PostgreSQL: {str(e)}")
        raise


def return_db_connection(conn, close=False):
    """
    Devolve conexão ao pool.
    
    Args:
        conn: Conexão a ser devolvida
        close: Se True, fecha a conexão em vez de devolver ao pool
    """
    _db_pool.return_connection(conn, close=close)


@contextmanager
def get_db_cursor(commit=True):
    """
    Context manager para obter cursor com gerenciamento automático de conexão.
    
    ✅ RECOMENDADO: Use este método para todas as queries!
    
    Args:
        commit: Se True (padrão), faz commit automático ao final
    
    Yields:
        cursor: Cursor do PostgreSQL
    
    Exemplo:
        with get_db_cursor() as cursor:
            cursor.execute("SELECT * FROM users WHERE id = %s", (user_id,))
            user = cursor.fetchone()
        
        # Múltiplas queries na mesma transação:
        with get_db_cursor(commit=False) as cursor:
            cursor.execute("INSERT INTO logs ...")
            cursor.execute("UPDATE users ...")
            cursor.connection.commit()  # Commit manual
    """
    conn = None
    cursor = None
    start_time = time.time()
    
    try:
        conn = _db_pool.get_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        yield cursor
        
        if commit:
            conn.commit()
        
        duration_ms = (time.time() - start_time) * 1000
        if duration_ms > 1000:  # Log queries lentas (> 1s)
            logger.warning(f"⚠️ Query lenta: {duration_ms:.0f}ms")
            
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"❌ Erro na query: {str(e)}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            _db_pool.return_connection(conn)


def init_db_pool():
    """
    Inicializa o pool de conexões.
    Chamado na inicialização da aplicação Flask.
    """
    _db_pool.initialize()


def close_db_pool():
    """
    Fecha o pool de conexões.
    Chamado no shutdown da aplicação Flask.
    """
    _db_pool.close_all()


def get_pool_stats():
    """Retorna estatísticas do pool de conexões"""
    return _db_pool.get_stats()


# =============================================================================
# COMPATIBILIDADE COM CÓDIGO LEGADO
# =============================================================================
# Função legacy que cria conexão direta (sem pool). NÃO usar em código novo.

def get_db_connection_legacy():
    """
    ⚠️ DEPRECATED: Use get_db_cursor() em vez disso!
    
    Esta função existe apenas para compatibilidade com código antigo.
    Cria uma nova conexão (sem pool) para casos específicos.
    """
    logger.warning("⚠️ get_db_connection_legacy() é deprecated. Use get_db_cursor()!")
    
    try:
        connection = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST'),
            database=os.getenv('POSTGRES_DB'),
            user=os.getenv('POSTGRES_USER'),
            password=os.getenv('POSTGRES_PASSWORD'),
            port=os.getenv('POSTGRES_PORT', 5432),
            sslmode=os.getenv('POSTGRES_SSL_MODE', 'prefer')
        )
        return connection
    except Exception as e:
        logger.error(f"❌ Erro ao conectar no PostgreSQL: {str(e)}")
        raise
