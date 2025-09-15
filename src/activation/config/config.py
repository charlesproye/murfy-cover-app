REASON_MAPPING = {
    "bmw": {
        "BMWFD_VEHICLE_NOT_ALLOWED": "Le véhicule n’est pas éligible à l’activation."
    },
    "stellantis": {
        "pending": "En attente - raison à chercher auprès de Mobilisight"
    },
    "high-mobility": {
            "unspecified": "Lorsqu’une activation échoue sans raison spécifique, ce motif est renvoyé. Pour de nombreuses marques, cela signifie que le véhicule n’est pas éligible.",
            "invalid client configuration": "Cela indique que votre projet n’a pas été configuré par notre équipe de support. Veuillez contacter votre interlocuteur.",
            "invalid VIN": "Le VIN n’est pas reconnu par le constructeur comme un véhicule éligible.",
            "ineligible VIN": "Le véhicule n’est pas éligible à l’activation.",
            "VIN is assigned to a different OEM fleet": "Le VIN est déjà activé par un autre client de l’OEM, ce qui empêche High Mobility de l’activer.",
            "VIN is assigned to a different app or project": "Le VIN est déjà activé dans un autre projet. L’activation multi-projets n’est pas possible pour ces marques.",
            "consent assignment missing": "Le consentement de l’opérateur de flotte envers High Mobility est manquant dans le portail de l’OEM.",
            "not added to ford portal": "Le consentement de l’opérateur de flotte envers High Mobility est manquant dans le portail de Ford.",
            "invalid OEM state: VIN revoke in progress": "Ford traite encore une demande précédente de révocation du VIN, qui doit être terminée avant qu’une nouvelle autorisation puisse être créée.",
            "VIN activation timeout" : "L’activation du VIN a expiré car le véhicule n’a pas pu être contacté après plusieurs jours de tentatives.",
            "telematics hardware error": "Il y a un problème avec le matériel télématique du véhicule. Veuillez contacter notre équipe de support.",
            "invalid OEM state": "Une erreur inconnue s’est produite dans l’API d’activation Renault. Veuillez contacter notre équipe de support.",
            "OEM fleet assignment failed": "Le VIN est très probablement déjà activé par un autre client de l’OEM. Veuillez contacter notre équipe de support.",
            "customer name tag missing":"Le nom de l’opérateur de flotte est manquant dans la demande d’autorisation. Réessayez en renseignant le tag vw-group-customer-name.",
            "privacy mode is enabled": "Le véhicule a activé le mode vie privée, ce qui bloque le processus d’activation. Nous continuerons à réessayer pendant plusieurs jours avant que le véhicule ne soit marqué comme rejeté.",
            "ineligible or constrained VIN": "Le véhicule n’est pas éligible à l’activation ou, dans de rares cas, son activation est bloquée par VW.",
            "activation timeout: no data": "Nous n’avons pas reçu de données du véhicule pendant le processus d’activation."
    },
    "volkswagen": {
        'in_validation': 'En attente davantage de détails côté Volkswagen',
        'enrollment_ongoing': 'Le véhicule a un utilisateur principal enregistré. Veuillez demander au conducteur de désenregistrer cet utilisateur principal. Une fois cela fait, veuillez réessayer d’enregistrer le véhicule.'
    },
}
