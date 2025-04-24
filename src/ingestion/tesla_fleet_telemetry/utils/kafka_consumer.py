import asyncio
import logging
from typing import AsyncGenerator, Optional, Dict, Any
import os
import json
import time
from pathlib import Path

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError
from aiokafka.consumer.consumer import ConsumerRecord
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

logger = logging.getLogger("kafka-consumer")


class KafkaConsumer:
    """
    Wrapper asynchrone pour la consommation de messages Kafka.
    Fournit une API simple pour consommer des messages de manière asynchrone.
    """
    
    def __init__(self, 
                 bootstrap_servers: str, 
                 topic: str, 
                 group_id: str,
                 auto_offset_reset: str = "latest",
                 max_poll_interval_ms: int = 300000,     # 5 minutes
                 session_timeout_ms: int = 60000,        # 1 minute
                 request_timeout_ms: int = 30000,        # 30 secondes
                 enable_auto_commit: bool = True,
                 auto_commit_interval_ms: int = 5000,   # 5 secondes
                 message_retention_hours: Optional[int] = 48):   # Rétention de 48 heures par défaut
        """
        Initialise le consumer Kafka avec les meilleures pratiques.
        
        Args:
            bootstrap_servers: Liste des serveurs Kafka
            topic: Nom du topic à consommer
            group_id: ID du groupe de consommateurs
            auto_offset_reset: Stratégie de reset d'offset ("earliest" ou "latest")
            max_poll_interval_ms: Intervalle maximum entre deux poll (ms)
            session_timeout_ms: Timeout de la session (ms)
            request_timeout_ms: Timeout des requêtes (ms)
            enable_auto_commit: Active le commit automatique des offsets
            auto_commit_interval_ms: Intervalle de commit automatique (ms)
            message_retention_hours: Durée de rétention des messages en heures (None pour ne pas modifier)
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.consumer = None
        self.running = False
        self.auto_offset_reset = auto_offset_reset
        self.message_retention_hours = message_retention_hours
        
        # Configuration Kafka optimisée
        self.config = {
            'bootstrap_servers': bootstrap_servers,
            'group_id': group_id,
            'auto_offset_reset': auto_offset_reset,
            'enable_auto_commit': enable_auto_commit,
            'auto_commit_interval_ms': auto_commit_interval_ms,
            'max_poll_interval_ms': max_poll_interval_ms,
            'session_timeout_ms': session_timeout_ms,
            'request_timeout_ms': request_timeout_ms,
            'heartbeat_interval_ms': 20000,       # 20 secondes
            'api_version': 'auto'
        }
        
        logger.info(f"KafkaConsumer initialisé: bootstrap_servers={bootstrap_servers}, topic={topic}, group_id={group_id}, auto_offset_reset={auto_offset_reset}")
        if message_retention_hours is not None:
            logger.info(f"Une politique de rétention de {message_retention_hours} heures sera appliquée au démarrage")
        logger.debug(f"Configuration complète: {self.config}")

    async def set_retention_policy(self, retention_hours: int = 48) -> bool:
        """
        Configure la politique de rétention des messages pour le topic.
        Les messages seront conservés pendant la durée spécifiée, puis supprimés.
        
        Args:
            retention_hours: Durée de rétention en heures (défaut: 48 heures)
            
        Returns:
            bool: True si la configuration a réussi, False sinon
        """
        retention_ms = retention_hours * 60 * 60 * 1000  # Conversion en millisecondes
        
        logger.info(f"Configuration de la rétention des messages à {retention_hours} heures ({retention_ms} ms) pour le topic {self.topic}")
        
        try:
            # Créer un client Admin Kafka
            admin_client = AIOKafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers,
                client_id=f"{self.group_id}-admin"
            )
            
            await admin_client.start()
            
            try:
                # Configurer la rétention pour le topic
                config_resource = {
                    "topic": self.topic,
                    "configs": {
                        "retention.ms": str(retention_ms)
                    }
                }
                
                await admin_client.alter_configs([config_resource])
                
                logger.info(f"Politique de rétention configurée avec succès: les messages seront conservés pendant {retention_hours} heures")
                return True
                
            finally:
                await admin_client.close()
                
        except Exception as e:
            logger.error(f"Erreur lors de la configuration de la politique de rétention: {str(e)}", exc_info=True)
            logger.warning(f"Pour configurer manuellement la rétention, utilisez la commande Kafka suivante:")
            logger.warning(f"  kafka-configs.sh --bootstrap-server {self.bootstrap_servers} --entity-type topics --entity-name {self.topic} --alter --add-config retention.ms={retention_ms}")
            return False

    async def _init_consumer(self) -> None:
        """
        Initialise et démarre le consumer AIOKafka avec gestion des erreurs.
        """
        if self.consumer is None:
            logger.info(f"Création du consumer AIOKafka pour topic: {self.topic}")
            
            try:
                self.consumer = AIOKafkaConsumer(
                    self.topic,
                    **self.config
                )
                
                logger.info(f"Démarrage du consumer AIOKafka...")
                await self.consumer.start()
                
                # Si une politique de rétention a été spécifiée, l'appliquer maintenant
                if self.message_retention_hours is not None:
                    success = await self.set_retention_policy(self.message_retention_hours)
                    if not success:
                        logger.warning(f"Impossible d'appliquer la politique de rétention de {self.message_retention_hours} heures")
                
                # Vérifier les partitions assignées
                partitions = self.consumer.assignment()
                logger.info(f"Partitions assignées: {partitions}")
                
                # Obtenir la position actuelle
                positions = {}
                for tp in partitions:
                    positions[tp] = await self.consumer.position(tp)
                logger.info(f"Positions actuelles: {positions}")
                
                # Obtenir le décalage de fin
                end_offsets = {}
                for tp in partitions:
                    end_offsets[tp] = await self.consumer.end_offsets([tp])
                logger.info(f"Offsets de fin: {end_offsets}")
                
                self.running = True
                logger.info(f"Consumer Kafka démarré sur le topic: {self.topic}")
                
            except Exception as e:
                logger.error(f"Erreur lors de l'initialisation du consumer AIOKafka: {str(e)}", exc_info=True)
                raise

    async def consume(self) -> AsyncGenerator[ConsumerRecord, None]:
        """
        Génère un flux de messages Kafka de manière asynchrone avec gestion des erreurs.
        
        Yields:
            ConsumerRecord: Messages Kafka consommés
        """
        try:
            await self._init_consumer()
            
            logger.info(f"Début de la consommation du topic {self.topic} avec auto_offset_reset={self.auto_offset_reset}")
            
            msg_count = 0
            last_log_time = asyncio.get_event_loop().time()
            
            while self.running:
                try:
                    # Récupérer les messages avec timeout
                    async for message in self.consumer:
                        msg_count += 1
                        current_time = asyncio.get_event_loop().time()
                        
                        # Log périodique toutes les 10 secondes
                        if current_time - last_log_time > 10:
                            logger.info(f"Consommation en cours: {msg_count} messages reçus jusqu'à présent")
                            last_log_time = current_time
                        
                        logger.debug(f"Message reçu: topic={message.topic}, partition={message.partition}, offset={message.offset}, timestamp={message.timestamp}")
                        yield message
                        
                        # Attendre brièvement pour éviter de surcharger la CPU
                        await asyncio.sleep(0)
                
                except KafkaError as e:
                    logger.error(f"Erreur Kafka pendant la consommation: {str(e)}")
                    # En cas d'erreur Kafka, attendre avant de réessayer
                    await asyncio.sleep(1)
                    
                    # Tenter de redémarrer le consumer si nécessaire
                    if not self.consumer._closed:
                        try:
                            await self.consumer.stop()
                        except:
                            pass
                        await self._init_consumer()
                
        except Exception as e:
            logger.error(f"Erreur inattendue dans le consumer Kafka: {str(e)}", exc_info=True)
            raise
        finally:
            await self.close()

    async def close(self) -> None:
        """
        Ferme proprement le consumer Kafka.
        """
        self.running = False
        
        if self.consumer is not None:
            try:
                logger.info("Fermeture du consumer Kafka...")
                await self.consumer.stop()
                logger.info("Consumer Kafka fermé avec succès")
            except Exception as e:
                logger.error(f"Erreur lors de la fermeture du consumer Kafka: {str(e)}", exc_info=True)
            finally:
                self.consumer = None 
