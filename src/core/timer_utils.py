import time
from contextlib import contextmanager
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)

class CodeTimer:
    """Timer simple pour mesurer les étapes du code"""
    
    def __init__(self, name="Code Execution"):
        self.name = name
        self.steps = {}
        self.current_step = None
        self.start_time = None
        
    @contextmanager
    def step(self, step_name: str):
        """Contexte manager pour mesurer une étape"""
        step_start = time.time()
        self.current_step = step_name
        logger.info(f"⏱️  Début: {step_name}")
        
        try:
            yield
        finally:
            step_duration = time.time() - step_start
            self.steps[step_name] = step_duration
            logger.info(f"✅ Fin: {step_name} ({step_duration:.2f}s)")
    
    def get_summary(self) -> Dict[str, float]:
        """Retourne un résumé des temps d'exécution"""
        total_time = sum(self.steps.values())
        
        logger.info(f"\n📊 RÉSUMÉ - {self.name}")
        logger.info("=" * 50)
        
        for step, duration in self.steps.items():
            percentage = (duration / total_time) * 100
            logger.info(f"{step:30} {duration:8.2f}s ({percentage:5.1f}%)")
        
        logger.info(f"{'TOTAL':30} {total_time:8.2f}s (100.0%)")
        logger.info("=" * 50)
        
        return self.steps
