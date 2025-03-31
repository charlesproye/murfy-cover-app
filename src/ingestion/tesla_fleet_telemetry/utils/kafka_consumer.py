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
                 auto_commit_interval_ms: int = 5000):   # 5 secondes
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
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.consumer = None
        self.running = False
        self.auto_offset_reset = auto_offset_reset
        
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
        logger.debug(f"Configuration complète: {self.config}")

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
