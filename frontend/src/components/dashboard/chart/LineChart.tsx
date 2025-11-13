'use client';

import React, { useMemo } from 'react';
import {
  Chart as ChartJS,
  LineElement,
  PointElement,
  LinearScale,
  Title,
  CategoryScale,
  Tooltip,
  Legend,
  TooltipItem,
  ScriptableContext,
} from 'chart.js';
import { Line } from 'react-chartjs-2';
import useGetDataGraph from '@/hooks/dashboard/passport/useGetDataGraph';
import { DataGraphRequestSwr } from '@/interfaces/dashboard/passport/DataGraph';
import LoadingSmall from '@/components/common/loading/loadingSmall';
import {
  getTrendlinePoints,
  calculateTrendlineY,
  parseTrendlineEquation,
} from '@/utils/regression';

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
);

const PREDICTION_DISTANCE = 30000;

const LineChart: React.FC<{ formatDate: string; vin: string | undefined }> = ({
  formatDate,
  vin,
}) => {
  const { data, isLoading }: DataGraphRequestSwr = useGetDataGraph(vin, formatDate);

  const currentVehiclePoints = useMemo(() => {
    if (!data?.data_points) return [];
    const points = data.data_points.filter((point) => point.is_current_vehicle);
    return points;
  }, [data]);

  const firstCurrentPoint = useMemo(() => {
    return currentVehiclePoints[0];
  }, [currentVehiclePoints]);

  const lastCurrentPoint = useMemo(() => {
    return currentVehiclePoints[currentVehiclePoints.length - 1];
  }, [currentVehiclePoints]);

  const cleanedPoints = useMemo(() => {
    if (!data?.data_points) return [];

    return data.data_points.map((point) => ({
      x: point.odometer,
      y: point.soh_vehicle,
    }));
  }, [data?.data_points]);

  useMemo(() => {
    if (cleanedPoints.length < 10) return null;
  }, [cleanedPoints]);

  // Extraction des coefficients pour les trendlines
  const coefficientsMin = data?.trendline_min
    ? parseTrendlineEquation(data.trendline_min)
    : undefined;
  const coefficientsMax = data?.trendline_max
    ? parseTrendlineEquation(data.trendline_max)
    : undefined;
  const coefficientsAvg = data?.trendline
    ? parseTrendlineEquation(data.trendline)
    : undefined;

  // Points de la courbe initiale (utilise la trendline moyenne avec offset)
  const initialCurvePoints = useMemo(() => {
    if (!firstCurrentPoint || !coefficientsAvg) return [];
    const points = [];
    const steps = 20;
    const trendValueAtFirstPoint = calculateTrendlineY(
      firstCurrentPoint.odometer,
      coefficientsAvg,
    );
    const offset = firstCurrentPoint.soh_vehicle - trendValueAtFirstPoint;
    for (let i = 0; i <= steps; i++) {
      const x = (i / steps) * firstCurrentPoint.odometer;
      const y = calculateTrendlineY(x, coefficientsAvg) + offset;
      points.push({ x, y });
    }
    points[points.length - 1] = {
      x: firstCurrentPoint.odometer,
      y: firstCurrentPoint.soh_vehicle,
    };
    return points;
  }, [firstCurrentPoint, coefficientsAvg]);

  // Points de la prédiction du véhicule actuel (utilise la trendline moyenne avec offset)
  const currentVehiclePredictionPoints = useMemo(() => {
    if (!lastCurrentPoint || !coefficientsAvg) return [];
    const points = [];
    const startOdometer = lastCurrentPoint.odometer;
    const endOdometer = startOdometer + PREDICTION_DISTANCE;
    const step = PREDICTION_DISTANCE / 20;
    points.push({ x: startOdometer, y: lastCurrentPoint.soh_vehicle });
    const trendValueAtLastPoint = calculateTrendlineY(startOdometer, coefficientsAvg);
    const offset = lastCurrentPoint.soh_vehicle - trendValueAtLastPoint;
    for (let x = startOdometer + step; x <= endOdometer; x += step) {
      points.push({
        x,
        y: calculateTrendlineY(x, coefficientsAvg) + offset,
      });
    }
    return points;
  }, [lastCurrentPoint, coefficientsAvg]);

  // Courbes de tendance min, max, moyenne (pour la flotte ou la marque)
  const completeTrendlineMinPoints = useMemo(() => {
    if (!data?.data_points?.length || !lastCurrentPoint || !coefficientsMin) return [];
    const maxOdometer = lastCurrentPoint.odometer + PREDICTION_DISTANCE;
    return getTrendlinePoints(0, maxOdometer, coefficientsMin);
  }, [data?.data_points, lastCurrentPoint, coefficientsMin]);

  const completeTrendlineMaxPoints = useMemo(() => {
    if (!data?.data_points?.length || !lastCurrentPoint || !coefficientsMax) return [];
    const maxOdometer = lastCurrentPoint.odometer + PREDICTION_DISTANCE;
    return getTrendlinePoints(0, maxOdometer, coefficientsMax);
  }, [data?.data_points, lastCurrentPoint, coefficientsMax]);

  const completeTrendlineAvgPoints = useMemo(() => {
    if (!data?.data_points?.length || !lastCurrentPoint || !coefficientsAvg) return [];
    const maxOdometer = lastCurrentPoint.odometer + PREDICTION_DISTANCE;
    return getTrendlinePoints(0, maxOdometer, coefficientsAvg);
  }, [data?.data_points, lastCurrentPoint, coefficientsAvg]);

  const chartData = useMemo(
    () => ({
      datasets: [
        // Tendance complète max (historique + prédiction)
        {
          label: 'Tendance Max',
          data: completeTrendlineMaxPoints,
          borderColor: 'rgba(220, 0, 0, 0.8)',
          borderWidth: 2.5,
          borderDash: [5, 5],
          tension: 0.2,
          fill: false,
          pointRadius: 0,
        },
        // Tendance moyenne (historique + prédiction)
        {
          label: 'Tendance Moyenne',
          data: completeTrendlineAvgPoints,
          borderColor: 'rgba(110, 110, 110, 0.5)',
          borderWidth: 2.5,
          borderDash: [5, 5],
          tension: 0.2,
          fill: false,
          pointRadius: 0,
        },
        // Tendance complète min (historique + prédiction)
        {
          label: 'Tendance Min',
          data: completeTrendlineMinPoints,
          borderColor: 'rgba(0, 180, 0, 0.8)',
          borderWidth: 2.5,
          borderDash: [5, 5],
          tension: 0.2,
          fill: false,
          pointRadius: 0,
        },
        // Points actuels du véhicule avec courbe initiale
        {
          label: 'Véhicule Actuel',
          data: [
            ...initialCurvePoints,
            ...currentVehiclePoints
              .filter((_, index, array) => {
                if (array.length <= 40) return true;
                const step = Math.floor(array.length / 40);
                return index % step === 0 || index === array.length - 1;
              })
              .map((point) => ({
                x: point.odometer,
                y: point.soh_vehicle,
              })),
          ],
          borderColor: '#2d67f6',
          backgroundColor: '#2d67f6',
          tension: 0.2,
          fill: false,
          pointStyle: (ctx: ScriptableContext<'line'>) => {
            const index = ctx.dataIndex;
            return index >= initialCurvePoints.length ? 'circle' : false;
          },
          pointRadius: (ctx: ScriptableContext<'line'>) => {
            const index = ctx.dataIndex;
            return index >= initialCurvePoints.length ? 4 : 0;
          },
          pointHoverRadius: (ctx: ScriptableContext<'line'>) => {
            const index = ctx.dataIndex;
            return index >= initialCurvePoints.length ? 6 : 0;
          },
          borderWidth: 2,
        },
        // Prédiction du véhicule actuel
        {
          label: 'Prédiction Véhicule',
          data: currentVehiclePredictionPoints,
          borderColor: '#2d67f6',
          borderWidth: 2,
          borderDash: [5, 5],
          tension: 0.2,
          fill: false,
          pointRadius: 0,
        },
      ],
    }),
    [
      currentVehiclePoints,
      initialCurvePoints,
      completeTrendlineMinPoints,
      completeTrendlineMaxPoints,
      currentVehiclePredictionPoints,
      completeTrendlineAvgPoints,
    ],
  );

  const chartScales = useMemo(() => {
    // Collecter toutes les valeurs de SoH pour déterminer les limites de l'axe Y
    const allValues = [
      // Points du véhicule actuel
      ...currentVehiclePoints.map((p) => p.soh_vehicle),
      // Points de la courbe initiale
      ...(initialCurvePoints?.map((p) => p.y) || []),
      // Points de prédiction
      ...(currentVehiclePredictionPoints?.map((p) => p.y) || []),
      // Points des tendances
      ...(completeTrendlineMinPoints?.map((p) => p.y) || []),
      ...(completeTrendlineMaxPoints?.map((p) => p.y) || []),
      ...(completeTrendlineAvgPoints?.map((p) => p.y) || []),
    ];

    // Valeur maximale avec une marge de 5% au-dessus
    const maxValue = Math.ceil(Math.max(...allValues)) + 5;
    // Valeur minimale pour assurer une bonne visualisation (avec marge)
    const minValue = Math.floor(Math.min(...allValues) - 5);
    const range = maxValue - minValue;
    const stepSize = Math.ceil(range / 5); // 5 graduations pour plus de précision

    // Odomètre maximum pour l'axe X
    const maxOdometer = lastCurrentPoint
      ? lastCurrentPoint.odometer + PREDICTION_DISTANCE
      : PREDICTION_DISTANCE;

    return {
      x: {
        type: 'linear' as const,
        ticks: {
          font: { size: 12 },
          callback: function (tickValue: number | string): string {
            const value =
              typeof tickValue === 'string' ? parseFloat(tickValue) : tickValue;
            return (value / 1000).toFixed(1) + 'K';
          },
        },
        grid: { display: false },
        border: { display: false },
        max: maxOdometer,
        min: 0, // S'assurer que l'axe X commence à 0 km
      },
      y: {
        beginAtZero: false,
        ticks: {
          stepSize: stepSize,
          font: { size: 12 },
          callback: function (tickValue: number | string): string {
            const value =
              typeof tickValue === 'string' ? parseFloat(tickValue) : tickValue;
            return Math.round(value) + '%';
          },
        },
        grid: { display: true },
        min: minValue,
        max: maxValue,
        border: { display: false },
      },
    };
  }, [
    currentVehiclePoints,
    lastCurrentPoint,
    currentVehiclePredictionPoints,
    completeTrendlineMinPoints,
    completeTrendlineMaxPoints,
    completeTrendlineAvgPoints,
    initialCurvePoints,
  ]);

  if (!vin || isLoading) {
    return <LoadingSmall />;
  }

  if (!data || !data.data_points || data.data_points.length === 0) {
    return <p>No data available</p>;
  }

  const options = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      title: {
        display: false,
      },
      legend: {
        display: false,
      },
      tooltip: {
        enabled: true,
        backgroundColor: 'rgba(0, 0, 0, 0.8)',
        callbacks: {
          label: (context: TooltipItem<'line'>) => {
            const label = context.dataset.label || '';
            const value = context.parsed.y;
            return `${label}: ${value.toFixed(1)}%`;
          },
        },
      },
    },
    scales: chartScales,
  };

  return <Line className="w-full h-[400px]" data={chartData} options={options} />;
};

export default LineChart;
