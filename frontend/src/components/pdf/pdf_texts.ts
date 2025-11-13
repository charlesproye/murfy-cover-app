export const pdfTexts = {
  // ENGLISH VERSION *********************
  EN: {
    mileage: 'MILEAGE',
    immatriculation: 'IMMATRICULATION',
    registration_date: 'REGISTRATION DATE',
    type: 'TYPE',
    version: 'VERSION',
    battery: 'BATTERY',
    battery_oem: 'BATTERY OEM',
    chemistry: 'CHEMISTRY',
    capacity: 'CAPACITY',
    wltp_range: 'WLTP RANGE',
    consumption: 'CONSUMPTION',
    soh_score_explanation:
      "The Bib Score compares the car's State-of-Health (SoH) to similar vehicles - similar odometer and age.",

    issued_date: 'Issued Date:',

    soh_chart: {
      title: 'Vehicle overview',
      subtitle: 'SoH evolution',
      legend: {
        vehicle_soh: 'Vehicle SoH',
        prediction: 'Prediction',
        odometer: 'Odometer (km)',
      },
      explanation:
        'SoH (State of Health) represents the health of your battery compared to its new condition. A new battery has an SoH of 100%. Natural degradation reduces this percentage over time and charging cycles.',
    },
    range: {
      title: 'Range',
      subtitle:
        "Bib compares the WLTP range announced by the manufacturer with the current range observed during the vehicle's monitoring by Bib Batteries.",
      usage: 'USAGE',
      urban: 'Urban',
      motorway: 'Motorway',
      mixed: 'Mixed used',
      summer: 'SUMMER (23°C)',
      winter: 'WINTER (0°C)',
    },
    warranty: {
      title: 'Battery Warranty',
      subtitle:
        'EVs come with two manufacturer warranties: the vehicle warranty and the battery warranty which ensures the battery maintains a certain level of capacity and performance.',
      remaining_warranty_mileage: 'REMAINING WARRANTY MILEAGE',
      remaining_warranty_time: 'REMAINING WARRANTY TIME',
      expected_range_at_warranty_end: 'EXPECTED RANGE AT WARRANTY END',
      expected_range_at_warranty_end_explanation: (
        warranty_date: number,
        warranty_km: string,
      ) =>
        `The original battery warranty of this vehicle guarantees at least 70% State of Health (SoH) for ${warranty_date} years or ${warranty_km} km, whichever comes first.`,
      missing_data: 'Missing data',
    },
    charging: {
      title: 'Charging Summary',
      type: 'TYPE',
      time_and_cost_of_charge: 'TIME AND COST OF CHARGE (10% - 80%)',
      typical: 'TYPICAL',
      power: 'POWER',
      explanation: (brand: string, model: string) =>
        `These data represent the different types of charge available for the vehicle ${brand} ${model}, classified by power and current type. Time and cost for a 100% charge from 10% to 80% are provided.`,
    },
    soh_information: {
      title: 'State of Health (SoH)',
      explanation:
        "The SoH is calculated by Bib using driving data, meaning raw data transmitted by the vehicle's BMS and stored in the manufacturer's cloud. This value does not come from a physical reading of the onboard computer nor from a manufacturer's calculation.",
      explanation_2:
        'Bib provides an independent SoH, which does not imply any manufacturer liability.',
    },
    soh_information_flash: {
      title: 'State of Health (SoH)',
      explanation:
        'The SoH is estimated +/-5% by Bib based on tens of thousands of vehicles already tracked, according to the global UN technical regulation on the durability of electric vehicle batteries',
      explanation_2:
        'Bib batteries also offers a +/-1% SoH measurement by tracking your vehicle for 7 consecutive days. For more information, contact us',
    },
    vehicle_and_battery_warranty: {
      title: 'Vehicle & Battery Warranty',
      explanation:
        'EVs are covered by two different manufacturer warranties: the vehicle warranty and the battery warranty which ensures the battery maintains a certain level of capacity and performance.',
      explanation_2:
        '• The vehicle warranty covers engine, powertrain, onboard electronics, and vehicle control systems, typically lasting 5 years.',
      explanation_3: (
        warranty_date: number | undefined,
        warranty_km: number | undefined,
      ) =>
        `• The battery warranty is related specifically to the electrical function and health of the battery. It guarantees a certain level of capacity and performance, usually more than 70% SoH. ${warranty_date && warranty_km ? `The battery warranty for this model is ${warranty_date} years or ${warranty_km} km, but it may vary by brand and manufacturer. More details can be found on the Bib battery warranty page.` : 'The standard model for the battery warranty is 8 years or 192000 km, but it may vary by brand and manufacturer. More details can be found on the Bib battery warranty page.'}`,
    },
    range_insights: {
      title: 'Range Insights',
      explanation:
        "Bib compares the WLTP (Worldwide Harmonized Light Vehicle Test Procedure) range, as announced by the manufacturer at the time of the vehicle's production, with the current range observed during monitoring of the vehicle by Bib Batteries. This comparison helps assess how the vehicle's battery performance has changed over time and how it aligns with the manufacturer's initial estimates, providing a more accurate understanding of the vehicle's current energy efficiency.",
    },
    report_validity: {
      title: 'Report Validity',
      explanation:
        'This Report verifies the performance and condition of the electric vehicle at the time of the testing based on the data provided during the monitoring of the vehicle. This Report is not a warranty or guarantee of the performance and condition of vehicle, and any modification made to the vehicle after the testing phase may invalidate this Report. Full terms and conditions can be found at',
    },
  },

  // FRENCH VERSION *********************
  FR: {
    mileage: 'KILOMETRAGE',
    immatriculation: 'IMMATRICULATION',
    registration_date: "DATE D'IMMATRICULATION",
    type: 'TYPE',
    version: 'VERSION',
    battery: 'BATTERIE',
    battery_oem: 'FABRICANT',
    chemistry: 'CHIMIE',
    capacity: 'CAPACITÉ',
    wltp_range: 'AUTONOMIE WLTP',
    consumption: 'CONSOMMATION',
    soh_score_explanation:
      "Le Bib Score compare l'état de santé (SoH) du véhicule à des véhicules similaires - même kilométrage et même âge.",

    issued_date: 'Date de délivrance:',

    soh_chart: {
      title: 'Batterie',
      subtitle: 'Évolution du SoH',
      legend: {
        vehicle_soh: 'SoH',
        prediction: 'Prédiction',
        odometer: 'Kilométrage (km)',
      },
      explanation:
        'Le SoH (État de Santé) représente la santé de votre batterie par rapport à son état neuf. Une batterie neuve a un SoH de 100%. La dégradation naturelle réduit ce pourcentage au fil du temps et des cycles de charge.',
    },
    range: {
      title: 'Autonomie',
      subtitle:
        "Bib compare l'autonomie WLTP annoncée par le constructeur avec l'autonomie réelle observée lors du suivi du véhicule par Bib Batteries.",
      usage: 'CYCLE',
      urban: 'Urbain',
      motorway: 'Autoroute',
      mixed: 'Mixte',
      summer: 'ÉTÉ (23°C)',
      winter: 'HIVER (0°C)',
    },
    warranty: {
      title: 'Garantie batterie',
      subtitle:
        'Les véhicules électriques sont assortis de deux garanties constructeur : la garantie véhicule et la garantie batterie, qui garantit que la batterie conserve un certain niveau de capacité et de performance.',
      remaining_warranty_mileage: 'KM RESTANT DE GARANTIE',
      remaining_warranty_time: 'TEMPS RESTANT DE GARANTIE',
      expected_range_at_warranty_end: 'AUTONOMIE ESPÉRÉE À LA FIN DE LA GARANTIE',
      expected_range_at_warranty_end_explanation: (
        warranty_date: number,
        warranty_km: string,
      ) =>
        `La garantie de la batterie originale de ce véhicule garantit au moins 70% d'état de santé (SoH) pour ${warranty_date} ans ou ${warranty_km} km, le premier événement arrivant.`,
      missing_data: 'Données manquantes',
    },
    charging: {
      title: 'Information sur la Recharge',
      type: 'TYPE',
      time_and_cost_of_charge: 'TEMPS ET COÛT DE RECHARGE (10% - 80%)',
      typical: 'TYPICAL',
      power: 'POWER',
      explanation: (brand: string, model: string) =>
        `Ces données représentent les différents types de recharge disponibles pour le véhicule ${brand} ${model}, classés par puissance et type de courant. Temps et coût pour une recharge de 100% de 10% à 80% sont fournis.`,
    },
    soh_information: {
      title: 'État de Santé (SoH)',
      explanation:
        "Le SoH est calculé par Bib en utilisant les données de conduite, c'est-à-dire les données brutes transmises par le BMS du véhicule et stockées dans le cloud du constructeur. Cette valeur ne provient pas d'une lecture physique de l'ordinateur de bord ni d'un calcul du constructeur.",
      explanation_2:
        "Bib fournit un SoH indépendant, qui n'implique aucune responsabilité du constructeur.",
    },
    soh_information_flash: {
      title: 'État de Santé (SoH)',
      explanation:
        'Le SoH est estimé à +/-5% par Bib à partir de dizaines de milliers de véhicules déjà suivis, conformément à la précision exigée par le règlement technique mondial ONU sur la durabilité des batteries des véhicules électriques',
      explanation_2:
        "Bib batteries propose également une mesure de SoH à +/-1% en suivant votre véhicule pendant 7 jours consécutifs. Pour plus d'information, contactez-nous",
    },
    vehicle_and_battery_warranty: {
      title: 'Garantie véhicule & batterie',
      explanation:
        'Les véhicules électriques sont assortis de deux garanties constructeur : la garantie véhicule et la garantie batterie, qui garantit que la batterie conserve un certain niveau de capacité et de performance.',
      explanation_2:
        "• La garantie véhicule couvre le moteur, la transmission, l'électronique embarquée et les systèmes de contrôle du véhicule, généralement valable 5 ans.",
      explanation_3: (
        warranty_date: number | undefined,
        warranty_km: number | undefined,
      ) =>
        `• La garantie batterie est spécifiquement liée au fonctionnement électrique et à la santé de la batterie. Elle garantit un certain niveau de capacité et de performance, généralement plus de 70% SoH. ${
          warranty_date && warranty_km
            ? `La garatie pour ce modèle est de ${warranty_date} ans ou ${warranty_km} km, mais il peut varier en fonction de la marque et du constructeur. Plus de détails peuvent être trouvés sur la page de garantie batterie de Bib.`
            : 'Le modèle standard pour la garantie batterie est de 8 ans ou 192000 km, mais il peut varier en fonction de la marque et du constructeur. Plus de détails peuvent être trouvés sur la page de garantie batterie de Bib.'
        }`,
    },
    range_insights: {
      title: "Informations sur l'autonomie",
      explanation:
        "Bib compare l'autonomie WLTP (Worldwide Harmonized Light Vehicle Test Procedure), telle qu'annoncée par le constructeur au moment de la production du véhicule, avec l'autonomie actuelle observée lors de la surveillance du véhicule par Bib Batteries. Cette comparaison permet d'évaluer l'évolution des performances de la batterie du véhicule au fil du temps et leur adéquation avec les estimations initiales du constructeur, ce qui permet de mieux comprendre l'efficacité énergétique actuelle du véhicule.",
    },
    report_validity: {
      title: 'Validité du rapport',
      explanation:
        "Ce rapport estime les performances et l'état du présent véhicule électrique au moment du test sur la base de données fournies lors du suivi du milliers d’autres véhicules similaires. Ce rapport ne constitue pas une garantie des performances et de l'état du véhicule, et toute modification apportée au véhicule après le test peut invalider ce rapport. Les conditions générales complètes sont disponibles sur",
    },
  },
};
