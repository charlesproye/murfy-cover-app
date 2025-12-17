import React from 'react';
import { View, Text, Svg, Line, Polyline, Circle, Path } from '@react-pdf/renderer';
import { getTrendlinePoints, parseTrendlineEquation, Point } from '@/lib/trendline';
import { GetGenerationInfo } from '@/interfaces/flash-reports';
import { pdfTexts } from '@/components/pdf/pdf_texts';
import { LanguageEnum } from '@/components/entities/flash-report/forms/schema';

const PdfSohChartFlash: React.FC<{ data: GetGenerationInfo }> = ({ data }) => {
  const texts = pdfTexts[data?.language || LanguageEnum.EN];
  // Récupérer les vraies valeurs du véhicule
  const mileage = data.vehicle_info.mileage;
  const sohCurrent = data.battery_info.soh ? data.battery_info.soh * 100 : 100;

  // Valeurs de départ et de prédiction
  const odoCurrent = mileage;
  const odoPred = odoCurrent + 30000; // Prédiction à +30 000 km

  // Dimensions du graphique
  const width = 550;
  const height = 190;
  const marginX = 30;
  const marginY = 20;

  // Axes comme sur le web
  const minY = Math.max(60, Math.floor(sohCurrent - 5)); // 5% en dessous de la valeur actuelle
  const maxY = Math.min(110, Math.ceil(sohCurrent + 5)); // 5% au-dessus de la valeur actuelle
  const minX = 0;
  const maxX = odoPred;

  // Fonctions pour convertir les valeurs en coordonnées SVG
  const getX = (odo: number): number =>
    marginX + ((odo - minX) / (maxX - minX)) * (width - 2 * marginX);
  const getY = (soh: number): number =>
    marginY + ((maxY - soh) / (maxY - minY)) * (height - 2 * marginY);

  // Parser les équations de trendline et générer les points
  const getTrendlinePointsFromEquation = (equation: string | undefined): Point[] => {
    if (!equation) return [];
    const coefficients = parseTrendlineEquation(equation);
    return getTrendlinePoints(minX, maxX, coefficients);
  };

  const trendlinePoints = getTrendlinePointsFromEquation(data.battery_info.trendline);
  const trendlineMinPoints = getTrendlinePointsFromEquation(
    data.battery_info.trendline_min,
  );
  const trendlineMaxPoints = getTrendlinePointsFromEquation(
    data.battery_info.trendline_max,
  );

  // Séparer les points de la trendline principale : avant et après le point actuel
  const trendlineHistoryPoints = trendlinePoints.filter((p) => p.x <= odoCurrent);
  const trendlinePredictionPoints = trendlinePoints.filter((p) => p.x >= odoCurrent);

  // Points SVG pour les courbes
  const svgTrendlineHistory =
    trendlineHistoryPoints.length > 0
      ? trendlineHistoryPoints.map((p) => `${getX(p.x)},${getY(p.y)}`).join(' ')
      : '';
  const svgTrendlinePrediction =
    trendlinePredictionPoints.length > 0
      ? trendlinePredictionPoints.map((p) => `${getX(p.x)},${getY(p.y)}`).join(' ')
      : '';
  const svgTrendlineMin =
    trendlineMinPoints.length > 0
      ? trendlineMinPoints.map((p) => `${getX(p.x)},${getY(p.y)}`).join(' ')
      : '';
  const svgTrendlineMax =
    trendlineMaxPoints.length > 0
      ? trendlineMaxPoints.map((p) => `${getX(p.x)},${getY(p.y)}`).join(' ')
      : '';

  // Créer le path pour la zone remplie entre trendlineMin et trendlineMax
  const getFillPath = (): string => {
    if (trendlineMinPoints.length === 0 || trendlineMaxPoints.length === 0) return '';

    // S'assurer que les deux arrays ont la même longueur et sont alignés
    const minLength = Math.min(trendlineMinPoints.length, trendlineMaxPoints.length);
    const minPoints = trendlineMinPoints.slice(0, minLength);
    const maxPoints = trendlineMaxPoints.slice(0, minLength);

    // Créer le path: commencer par le premier point de max
    const firstMaxPoint = maxPoints[0];
    let path = `M ${getX(firstMaxPoint.x)},${getY(firstMaxPoint.y)}`;

    // Ajouter tous les points de max
    for (let i = 1; i < maxPoints.length; i++) {
      path += ` L ${getX(maxPoints[i].x)},${getY(maxPoints[i].y)}`;
    }

    // Ajouter tous les points de min en sens inverse
    for (let i = minPoints.length - 1; i >= 0; i--) {
      path += ` L ${getX(minPoints[i].x)},${getY(minPoints[i].y)}`;
    }

    // Fermer le path
    path += ' Z';

    return path;
  };

  const fillPath = getFillPath();

  // Point actuel - utiliser la valeur SoH réelle
  const currentPoint = { x: odoCurrent, y: sohCurrent };

  // Graduation Y (ticks) - calculée dynamiquement
  const yRange = maxY - minY;
  const yStep = Math.ceil(yRange / 6); // 6 graduations
  const yTicks = [];
  for (let i = Math.ceil(maxY / yStep) * yStep; i >= minY; i -= yStep) {
    yTicks.push(i);
  }

  // Graduation X (ticks)
  const xTicks = [
    0,
    Math.round(maxX / 4),
    Math.round(maxX / 2),
    Math.round((3 * maxX) / 4),
    maxX,
  ];

  return (
    <View
      style={{
        backgroundColor: '#fff',
        borderRadius: 12,
        paddingLeft: 16,
        paddingRight: 16,
        paddingTop: 10,
        paddingBottom: 10,
        border: '1px solid #E5E7EB',
      }}
    >
      {/* Header */}
      <View
        style={{
          flexDirection: 'row',
          justifyContent: 'space-between',
          alignItems: 'flex-start',
          paddingTop: 0,
          paddingBottom: 6,
          marginBottom: 0,
        }}
      >
        {/* Titre et sous-titre à gauche */}
        <View style={{ flexDirection: 'column', alignItems: 'flex-start', flex: 1 }}>
          <Text
            style={{
              fontSize: 14,
              fontWeight: 'bold',
              color: '#111827',
            }}
          >
            {texts.soh_chart.title}
          </Text>
          <Text style={{ fontSize: 9, color: '#2d6d49' }}>
            {texts.soh_chart.subtitle}
          </Text>
        </View>
        {/* Légende à droite */}
        <View
          style={{ flexDirection: 'row', alignItems: 'center', gap: 4, marginLeft: 8 }}
        >
          <View style={{ flexDirection: 'row', alignItems: 'center', marginRight: 8 }}>
            <View
              style={{
                width: 8,
                height: 8,
                borderRadius: 4,
                backgroundColor: '#2d6d49',
                marginRight: 2,
              }}
            />
            <Text style={{ fontSize: 9, color: '#111827', marginRight: 6 }}>
              {texts.soh_chart.legend.vehicle_soh}
            </Text>
            <View
              style={{
                width: 16,
                height: 0,
                borderTop: '2px dashed #2d6d49',
                marginRight: 2,
              }}
            />
            <Text style={{ fontSize: 9, color: '#888' }}>
              {texts.soh_chart.legend.prediction}
            </Text>
          </View>
        </View>
      </View>

      {/* Graphique SVG */}
      <View
        style={{
          alignItems: 'center',
          justifyContent: 'center',
          width: '100%',
        }}
      >
        <Svg width={width} height={height}>
          {/* Grille Y */}
          {yTicks.map((y, i) => (
            <Line
              key={y}
              x1={marginX}
              y1={getY(y)}
              x2={width - marginX}
              y2={getY(y)}
              stroke="#E5E7EB"
              strokeWidth={1}
              strokeDasharray={i === 0 || i === yTicks.length - 1 ? undefined : '2 2'}
            />
          ))}
          {/* Grille X supprimée, on garde seulement les labels */}
          {/* Axes principaux */}
          <Line
            x1={marginX}
            y1={height - marginY}
            x2={width - marginX}
            y2={height - marginY}
            stroke="#D1D5DB"
            strokeWidth={1}
          />
          <Line
            x1={marginX}
            y1={marginY}
            x2={marginX}
            y2={height - marginY}
            stroke="#D1D5DB"
            strokeWidth={1}
          />
          {/* Zone remplie entre trendlineMin et trendlineMax */}
          {fillPath && <Path d={fillPath} fill="#F3F4F6" />}
          {/* TrendlineMax - dashed green line */}
          {svgTrendlineMax && (
            <Polyline
              points={svgTrendlineMax}
              fill="none"
              stroke="#22c55e"
              strokeWidth={2}
              strokeDasharray="4 4"
            />
          )}
          {/* Trendline - solid black line (history) */}
          {svgTrendlineHistory && (
            <Polyline
              points={svgTrendlineHistory}
              fill="none"
              stroke="#2d6d49"
              strokeWidth={2}
            />
          )}
          {/* Trendline - dashed black line (prediction) */}
          {svgTrendlinePrediction && (
            <Polyline
              points={svgTrendlinePrediction}
              fill="none"
              stroke="#2d6d49"
              strokeWidth={2}
              strokeDasharray="4 4"
            />
          )}
          {/* TrendlineMin - dashed red line */}
          {svgTrendlineMin && (
            <Polyline
              points={svgTrendlineMin}
              fill="none"
              stroke="#ef4444"
              strokeWidth={2}
              strokeDasharray="4 4"
            />
          )}
          {/* Point actuel */}
          <Circle
            cx={getX(currentPoint.x)}
            cy={getY(currentPoint.y)}
            r={4}
            fill="#2d6d49"
          />
          {/* Labels Y */}
          {yTicks.map((y) => (
            <Text
              key={y}
              x={marginX - 24}
              y={getY(y) + 3}
              style={{ fontSize: 8, fill: '#888' }}
            >
              {y}%
            </Text>
          ))}
          {/* Labels X */}
          {xTicks.map((x) => (
            <Text
              key={x}
              x={getX(x) - 10}
              y={height - marginY + 16}
              style={{ fontSize: 8, fill: '#888' }}
            >
              {x}
            </Text>
          ))}
        </Svg>
      </View>

      <View
        style={{
          textAlign: 'right',
          width: '100%',
          marginBottom: 4,
        }}
      >
        <Text style={{ fontSize: 9, color: '#2d6d49' }}>
          {texts.soh_chart.legend.odometer}
        </Text>
      </View>

      {/* Texte d'explication */}
      <View style={{ borderTop: '1px solid #E5E7EB', paddingTop: 4 }}>
        <Text style={{ fontSize: 8, color: '#6B7280', marginBottom: 1 }}>
          {texts.soh_chart.explanation}
        </Text>
      </View>
    </View>
  );
};

export default PdfSohChartFlash;
