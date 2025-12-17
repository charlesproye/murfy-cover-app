import React from 'react';
import { View, Text, Svg, Line, Polyline, Circle } from '@react-pdf/renderer';
import { InfoVehicleResult } from '@/interfaces/dashboard/passport/infoVehicle';
import { calculateTrendlineY, getTrendlinePoints } from '@/lib/trendline';
import { pdfTexts } from '@/components/pdf/pdf_texts';
import { LanguageEnum } from '@/components/entities/flash-report/forms/schema';

const PdfSohChart: React.FC<{ data: InfoVehicleResult }> = ({ data }) => {
  const texts = pdfTexts[LanguageEnum.EN];
  // Récupérer les vraies valeurs du véhicule
  const mileage = data.vehicle_info?.odometer ?? 0;
  const sohCurrent = data.battery_info?.soh ?? 100;

  // Valeurs de départ et de prédiction
  // const odoStart = 0; // supprimé car inutilisé
  const odoCurrent = mileage;
  const odoPred = odoCurrent + 30000; // Prédiction à +30 000 km

  // Dimensions du graphique
  const width = 550;
  const height = 200;
  const margin = 30;

  // Axes comme sur le web
  const minY = Math.max(60, Math.floor(sohCurrent - 5)); // 5% en dessous de la valeur actuelle
  const maxY = Math.min(110, Math.ceil(sohCurrent + 5)); // 5% au-dessus de la valeur actuelle
  const minX = 0;
  const maxX = odoPred;

  // Fonctions pour convertir les valeurs en coordonnées SVG
  const getX = (odo: number): number =>
    margin + ((odo - minX) / (maxX - minX)) * (width - 2 * margin);
  const getY = (soh: number): number =>
    margin + ((maxY - soh) / (maxY - minY)) * (height - 2 * margin);

  // Générer les points de la courbe historique (de 0 à mileage)
  const historyPoints = getTrendlinePoints(minX, odoCurrent);
  // Générer les points de la prédiction (de mileage à mileage+30k)
  const predictionPoints = getTrendlinePoints(odoCurrent, odoPred);

  // Décaler la courbe pour qu'elle passe par le point actuel
  const trendValueAtCurrent = calculateTrendlineY(odoCurrent);
  const offset = sohCurrent - trendValueAtCurrent;
  const adjustedHistory = historyPoints.map((p) => ({ x: p.x, y: p.y + offset }));
  const adjustedPrediction = predictionPoints.map((p) => ({ x: p.x, y: p.y + offset }));

  // Points SVG
  const svgHistory = adjustedHistory.map((p) => `${getX(p.x)},${getY(p.y)}`).join(' ');
  const svgPrediction = adjustedPrediction
    .map((p) => `${getX(p.x)},${getY(p.y)}`)
    .join(' ');
  const currentPoint = adjustedHistory[adjustedHistory.length - 1];

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
        // marginBottom: 4,
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
          paddingBottom: 0,
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
      <View style={{ alignItems: 'center', justifyContent: 'center', width: '100%' }}>
        <Svg width={width} height={height} style={{ marginBottom: 4 }}>
          {/* Grille Y */}
          {yTicks.map((y, i) => (
            <Line
              key={y}
              x1={margin}
              y1={getY(y)}
              x2={width - margin}
              y2={getY(y)}
              stroke="#E5E7EB"
              strokeWidth={1}
              strokeDasharray={i === 0 || i === yTicks.length - 1 ? undefined : '2 2'}
            />
          ))}
          {/* Grille X supprimée, on garde seulement les labels */}
          {/* Axes principaux */}
          <Line
            x1={margin}
            y1={height - margin}
            x2={width - margin}
            y2={height - margin}
            stroke="#D1D5DB"
            strokeWidth={1}
          />
          <Line
            x1={margin}
            y1={margin}
            x2={margin}
            y2={height - margin}
            stroke="#D1D5DB"
            strokeWidth={1}
          />
          {/* Courbe historique */}
          <Polyline points={svgHistory} fill="none" stroke="#2d6d49" strokeWidth={2} />
          {/* Courbe prédiction (pointillée) */}
          <Polyline
            points={svgPrediction}
            fill="none"
            stroke="#2d6d49"
            strokeWidth={2}
            strokeDasharray="4 4"
          />
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
              x={margin - 24}
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
              y={height - margin + 16}
              style={{ fontSize: 8, fill: '#888' }}
            >
              {(x / 1000).toFixed(1)}K
            </Text>
          ))}
        </Svg>
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

export default PdfSohChart;
