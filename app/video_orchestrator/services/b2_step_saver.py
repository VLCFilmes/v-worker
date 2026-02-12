"""
💾 B2 Step Saver Service - Salva outputs do pipeline no Backblaze B2

Salva o output de cada step do Video Orchestrator Pipeline no B2:
- Payloads intermediários (JSON)
- Metadados de processamento
- Logs de cada step

Estrutura no B2:
users/{user_id}/projects/p_{project_id}_c_{conversation_id}/metadata/jobs/{job_id}/
├── step_01_normalize.json
├── step_02_concat.json
├── step_03_analyze.json
├── step_04_detect_silence.json
├── step_05_silence_cut.json
├── step_06_transcribe.json
├── step_07_phrase_grouping.json
├── step_08_classify.json
├── step_09_png_generation.json
├── step_10_shadow.json
├── step_11_positioning.json
├── step_12_payload_builder.json
└── step_15_render.json
"""

import os
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# Configurações do B2
B2_ENDPOINT = os.environ.get('B2_ENDPOINT', 'https://s3.us-west-000.backblazeb2.com')
B2_KEY_ID = os.environ.get('B2_KEY_ID')
B2_APP_KEY = os.environ.get('B2_APP_KEY')
B2_BUCKET = os.environ.get('B2_BUCKET', 'vinicius-ai-production')


class B2StepSaver:
    """
    Serviço para salvar outputs de steps do pipeline no Backblaze B2.
    
    Features:
    - Salva JSON de cada step
    - Gera URLs assinadas para download
    - Registra no banco (job_step_outputs)
    - Estrutura organizada por usuário/projeto/job
    """
    
    def __init__(self, db_connection_func=None):
        """
        Args:
            db_connection_func: Função para obter conexão com banco
        """
        self.db_connection_func = db_connection_func
        
        # Inicializar cliente B2
        self.s3_client = None
        if B2_KEY_ID and B2_APP_KEY:
            try:
                self.s3_client = boto3.client(
                    's3',
                    endpoint_url=B2_ENDPOINT,
                    aws_access_key_id=B2_KEY_ID,
                    aws_secret_access_key=B2_APP_KEY
                )
                logger.info("✅ [B2StepSaver] Cliente B2 inicializado")
            except Exception as e:
                logger.error(f"❌ [B2StepSaver] Erro ao inicializar cliente B2: {e}")
                self.s3_client = None
        else:
            logger.warning("⚠️ [B2StepSaver] B2 não configurado (B2_KEY_ID ou B2_APP_KEY ausente)")
    
    def save_step_output(
        self,
        job_id: str,
        step_number: int,
        step_name: str,
        output_data: Dict[str, Any],
        user_id: str,
        project_id: str = None,
        conversation_id: str = None
    ) -> Optional[str]:
        """
        Salva output de um step no B2 e registra no banco.
        
        Args:
            job_id: ID do job
            step_number: Número do step (1, 2, 3...)
            step_name: Nome do step (normalize, transcribe, etc)
            output_data: Dados a salvar (será convertido para JSON)
            user_id: ID do usuário
            project_id: ID do projeto (opcional)
            conversation_id: ID da conversa (opcional)
            
        Returns:
            URL do arquivo salvo no B2 ou None se erro
        """
        if not self.s3_client:
            logger.warning(f"⚠️ [B2StepSaver] B2 não disponível, pulando save do step {step_name}")
            return None
        
        try:
            # Gerar path no B2
            b2_path = self._generate_b2_path(
                user_id=user_id,
                project_id=project_id,
                conversation_id=conversation_id,
                job_id=job_id,
                step_number=step_number,
                step_name=step_name
            )
            
            logger.info(f"💾 [B2StepSaver] Salvando step {step_number} ({step_name}) em {b2_path}")
            
            # Converter para JSON
            json_data = json.dumps(output_data, ensure_ascii=False, indent=2, default=str)
            json_bytes = json_data.encode('utf-8')
            file_size = len(json_bytes)
            
            # Upload para B2
            self.s3_client.put_object(
                Bucket=B2_BUCKET,
                Key=b2_path,
                Body=json_bytes,
                ContentType='application/json',
                Metadata={
                    'job_id': job_id,
                    'step_number': str(step_number),
                    'step_name': step_name,
                    'timestamp': datetime.utcnow().isoformat()
                }
            )
            
            logger.info(f"✅ [B2StepSaver] Step salvo: {file_size} bytes")
            
            # Gerar URL assinada (válida por 7 dias)
            signed_url = self._generate_signed_url(b2_path, expiration=604800)
            
            # Registrar no banco
            self._register_in_database(
                job_id=job_id,
                step_number=step_number,
                step_name=step_name,
                b2_url=signed_url,
                b2_path=b2_path,
                file_size_bytes=file_size,
                output_summary=self._generate_summary(output_data)
            )
            
            return signed_url
            
        except ClientError as e:
            logger.error(f"❌ [B2StepSaver] Erro do B2: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ [B2StepSaver] Erro ao salvar step: {type(e).__name__}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    def _generate_b2_path(
        self,
        user_id: str,
        project_id: Optional[str],
        conversation_id: Optional[str],
        job_id: str,
        step_number: int,
        step_name: str
    ) -> str:
        """
        Gera path no B2 seguindo a estrutura definida.
        
        Formato:
        users/{user_id}/projects/p_{project_id}_c_{conversation_id}/metadata/jobs/{job_id}/step_XX_{name}.json
        
        Args:
            user_id: ID do usuário
            project_id: ID do projeto (pode ser None)
            conversation_id: ID da conversa (pode ser None)
            job_id: ID do job
            step_number: Número do step
            step_name: Nome do step
            
        Returns:
            Path completo no B2
        """
        # Base path
        base = f"users/{user_id}/projects"
        
        # Project folder
        if project_id and conversation_id:
            project_folder = f"p_{project_id}_c_{conversation_id}"
        elif project_id:
            project_folder = f"p_{project_id}_c_none"
        else:
            project_folder = f"p_none_c_{conversation_id or 'none'}"
        
        # Full path
        step_filename = f"step_{step_number:02d}_{step_name}.json"
        return f"{base}/{project_folder}/metadata/jobs/{job_id}/{step_filename}"
    
    def _generate_signed_url(self, b2_path: str, expiration: int = 604800) -> str:
        """
        Gera URL assinada para download do arquivo.
        
        Args:
            b2_path: Path do arquivo no B2
            expiration: Tempo de validade em segundos (default: 7 dias)
            
        Returns:
            URL assinada
        """
        try:
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': B2_BUCKET,
                    'Key': b2_path
                },
                ExpiresIn=expiration
            )
            return url
        except Exception as e:
            logger.error(f"❌ [B2StepSaver] Erro ao gerar URL assinada: {e}")
            return f"{B2_ENDPOINT}/{B2_BUCKET}/{b2_path}"
    
    def _generate_summary(self, output_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Gera resumo do output para armazenar no banco.
        
        Extrai informações chave sem precisar baixar o JSON completo.
        
        Args:
            output_data: Dados completos do output
            
        Returns:
            Dict com resumo
        """
        summary = {}
        
        try:
            # Total de itens (palavras, frases, etc)
            if 'words' in output_data:
                summary['total_words'] = len(output_data['words'])
            
            if 'phrases' in output_data or 'phrase_groups' in output_data:
                phrases = output_data.get('phrases') or output_data.get('phrase_groups', [])
                summary['total_phrases'] = len(phrases)
            
            if 'pngs' in output_data:
                summary['total_pngs'] = len(output_data['pngs'])
            
            # Duração
            if 'duration' in output_data:
                summary['duration_seconds'] = output_data['duration']
            
            # Status
            if 'status' in output_data:
                summary['status'] = output_data['status']
            
            # Tamanho do JSON
            json_size = len(json.dumps(output_data, default=str))
            summary['file_size_bytes'] = json_size
            
        except Exception as e:
            logger.error(f"❌ [B2StepSaver] Erro ao gerar summary: {e}")
        
        return summary
    
    def _register_in_database(
        self,
        job_id: str,
        step_number: int,
        step_name: str,
        b2_url: str,
        b2_path: str,
        file_size_bytes: int,
        output_summary: Dict[str, Any]
    ):
        """
        Registra o output salvo na tabela job_step_outputs.
        
        Args:
            job_id: ID do job
            step_number: Número do step
            step_name: Nome do step
            b2_url: URL assinada do arquivo
            b2_path: Path no B2
            file_size_bytes: Tamanho do arquivo
            output_summary: Resumo do output
        """
        if not self.db_connection_func:
            logger.warning("⚠️ [B2StepSaver] db_connection_func não disponível, pulando registro no banco")
            return
        
        db_conn = None
        db_cursor = None
        
        try:
            db_conn = self.db_connection_func()
            db_cursor = db_conn.cursor()
            
            db_cursor.execute("""
                INSERT INTO job_step_outputs (
                    job_id,
                    step_number,
                    step_name,
                    b2_url,
                    b2_path,
                    file_size_bytes,
                    output_summary,
                    status,
                    created_at,
                    completed_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, 'completed', NOW(), NOW()
                )
                ON CONFLICT (job_id, step_number) 
                DO UPDATE SET
                    b2_url = EXCLUDED.b2_url,
                    b2_path = EXCLUDED.b2_path,
                    file_size_bytes = EXCLUDED.file_size_bytes,
                    output_summary = EXCLUDED.output_summary,
                    status = 'completed',
                    completed_at = NOW()
            """, (
                job_id,
                step_number,
                step_name,
                b2_url,
                b2_path,
                file_size_bytes,
                json.dumps(output_summary)
            ))
            
            db_conn.commit()
            logger.info(f"✅ [B2StepSaver] Registrado no banco: job_step_outputs")
            
        except Exception as e:
            logger.error(f"❌ [B2StepSaver] Erro ao registrar no banco: {e}")
            if db_conn:
                db_conn.rollback()
        finally:
            if db_cursor:
                try:
                    db_cursor.close()
                except:
                    pass
            if db_conn:
                try:
                    db_conn.close()
                except:
                    pass

