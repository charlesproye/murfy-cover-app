import numpy as np
from scipy.optimize import minimize
from .config import *

def detecter_outliers(soh_values, method="iqr"):
    """
    Détecte les outliers dans une série de données.
    Paramètres :
        - soh_values : Liste des valeurs de SoH brutes.
        - method : Méthode de détection ("iqr" ou "std").
    Retourne :
        - Liste des indices non considérés comme outliers.
    """
    soh_values = np.array(soh_values)
    if method == "iqr":
        # Méthode de l'écart interquartile (IQR)
        q1, q3 = np.percentile(soh_values, [25, 75])
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
    elif method == "std":
        # Méthode basée sur l'écart-type
        mean = np.mean(soh_values)
        std_dev = np.std(soh_values)
        lower_bound = mean - 3 * std_dev
        upper_bound = mean + 3 * std_dev
    else:
        raise ValueError("Méthode non reconnue. Utilisez 'iqr' ou 'std'.")
    
    # Indices des valeurs non considérées comme outliers
    return [i for i, val in enumerate(soh_values) if lower_bound <= val <= upper_bound]

def optimiser_soh(soh_brut):
    """
    Ajuste les valeurs de SoH pour garantir une décroissance tout en minimisant 
    l'écart avec les valeurs brutes.
    """
    n = len(soh_brut)
    
    # Fonction objectif : minimiser l'écart quadratique entre les valeurs ajustées et les valeurs brutes
    def objectif(soh_adjusted):
        return np.sum((soh_adjusted - soh_brut) ** 2)
    
   # Contraintes : SoH doit être non-croissant et l'écart hebdomadaire doit être inférieur à 0,1%
    contraintes = [
        {'type': 'ineq', 'fun': lambda x, i=i: x[i - 1] - x[i]} for i in range(1, n)
    ] + [
        {'type': 'ineq', 'fun': lambda x, i=i: 0.001 * x[i - 1] - (x[i - 1] - x[i])} for i in range(1, n)
    ]
    
    # Borne supérieure et inférieure (par exemple, entre 0 et 100)
    bornes = [(0, 100) for _ in range(n)]
    
    # Initialisation des valeurs ajustées (on commence par les valeurs brutes)
    initial_guess = soh_brut.copy()
    
    # Résolution de l'optimisation
    result = minimize(objectif, initial_guess, method='SLSQP', constraints=contraintes, bounds=bornes)
    
    if result.success:
        return result.x  # Retourne les valeurs ajustées
    else:
        raise ValueError("L'optimisation a échoué : ", result.message)

def process_soh_for_vehicle(group):
    """
    Traite les données SoH pour un véhicule spécifique.
    """
    soh_values = group['soh'].values
    
    # Détection des outliers
    indices_valides = detecter_outliers(soh_values, method="iqr")
    soh_nettoye = np.array([soh_values[i] for i in indices_valides])
    
    # Optimisation
    if len(soh_nettoye) > 0:
        soh_optimise = optimiser_soh(soh_nettoye)
        
        # Création d'un nouveau DataFrame avec les valeurs optimisées
        result = group.iloc[indices_valides].copy()
        result['soh'] = soh_optimise
        return result
    return group


