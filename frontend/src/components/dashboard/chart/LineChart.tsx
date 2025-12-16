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

const LineChart: React.FC<{ vin: string | undefined }> = ({ vin }) => {
  const { data, isLoading }: DataGraphRequestSwr = useGetDataGraph(vin);

  // Extract coefficients for trendlines
  const coefficientsMin = data?.trendline_min
    ? parseTrendlineEquation(data.trendline_min)
    : undefined;
  const coefficientsMax = data?.trendline_max
    ? parseTrendlineEquation(data.trendline_max)
    : undefined;
  const coefficientsAvg = data?.trendline
    ? parseTrendlineEquation(data.trendline)
    : undefined;

  // Initial curve points (use average trendline with offset)
  const initialCurvePoints = useMemo(() => {
    if (!data?.data_points?.length || !coefficientsAvg) return [];
    const points = [];
    const steps = 20;
    const trendValueAtFirstPoint = calculateTrendlineY(
      data.data_points[0].odometer,
      coefficientsAvg,
    );
    const offset = data.data_points[0].soh - trendValueAtFirstPoint;
    for (let i = 0; i <= steps; i++) {
      const x = (i / steps) * data.data_points[0].odometer;
      const y = calculateTrendlineY(x, coefficientsAvg) + offset;
      points.push({ x, y });
    }
    points[points.length - 1] = {
      x: data.data_points[0].odometer,
      y: data.data_points[0].soh,
    };
    return points;
  }, [data, coefficientsAvg]);

  // Current vehicle prediction points (use average trendline with offset)
  const currentVehiclePredictionPoints = useMemo(() => {
    if (!data?.data_points?.length || !coefficientsAvg) return [];
    const lastDataPoint = data.data_points[data.data_points.length - 1];
    const points = [];
    const startOdometer = lastDataPoint.odometer;
    const endOdometer = startOdometer + PREDICTION_DISTANCE;
    const step = PREDICTION_DISTANCE / 20;
    points.push({ x: startOdometer, y: lastDataPoint.soh });
    const trendValueAtLastPoint = calculateTrendlineY(startOdometer, coefficientsAvg);
    const offset = lastDataPoint.soh - trendValueAtLastPoint;
    for (let x = startOdometer + step; x <= endOdometer; x += step) {
      points.push({
        x,
        y: calculateTrendlineY(x, coefficientsAvg) + offset,
      });
    }
    return points;
  }, [data, coefficientsAvg]);

  // Trendline min, max, average curves (for fleet or brand)
  const completeTrendlineMinPoints = useMemo(() => {
    if (!data?.data_points?.length || !coefficientsMin) return [];
    const maxOdometer =
      data.data_points[data.data_points.length - 1].odometer + PREDICTION_DISTANCE;
    return getTrendlinePoints(0, maxOdometer, coefficientsMin);
  }, [data, coefficientsMin]);

  const completeTrendlineMaxPoints = useMemo(() => {
    if (!data?.data_points?.length || !coefficientsMax) return [];
    const maxOdometer =
      data.data_points[data.data_points.length - 1].odometer + PREDICTION_DISTANCE;
    return getTrendlinePoints(0, maxOdometer, coefficientsMax);
  }, [data, coefficientsMax]);

  const completeTrendlineAvgPoints = useMemo(() => {
    if (!data?.data_points?.length || !coefficientsAvg) return [];
    const maxOdometer =
      data.data_points[data.data_points.length - 1].odometer + PREDICTION_DISTANCE;
    return getTrendlinePoints(0, maxOdometer, coefficientsAvg);
  }, [data, coefficientsAvg]);

  const chartData = useMemo(
    () => ({
      datasets: [
        // Complete max trendline (history + prediction)
        {
          label: 'Trendline Max',
          data: completeTrendlineMaxPoints,
          borderColor: 'rgba(0, 180, 0, 0.8)',
          borderWidth: 2.5,
          borderDash: [5, 5],
          tension: 0.2,
          fill: false,
          pointRadius: 0,
        },
        // Average trendline (history + prediction)
        {
          label: 'Trendline Avg',
          data: completeTrendlineAvgPoints,
          borderColor: 'rgba(110, 110, 110, 0.5)',
          borderWidth: 2.5,
          borderDash: [5, 5],
          tension: 0.2,
          fill: false,
          pointRadius: 0,
        },
        // Complete min trendline (history + prediction)
        {
          label: 'Trendline Min',
          data: completeTrendlineMinPoints,
          borderColor: 'rgba(220, 0, 0, 0.8)',
          borderWidth: 2.5,
          borderDash: [5, 5],
          tension: 0.2,
          fill: false,
          pointRadius: 0,
        },
        // Current vehicle points with initial curve
        {
          label: 'Current Vehicle',
          data: [
            ...initialCurvePoints,
            ...(data?.data_points?.length ? data.data_points : [])
              .filter((_, index, array) => {
                if (array.length <= 40) return true;
                const step = Math.floor(array.length / 40);
                return index % step === 0 || index === array.length - 1;
              })
              .map((point) => ({
                x: point.odometer,
                y: point.soh,
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
        {
          label: 'Current Vehicle Prediction',
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
      data,
      initialCurvePoints,
      completeTrendlineMinPoints,
      completeTrendlineMaxPoints,
      currentVehiclePredictionPoints,
      completeTrendlineAvgPoints,
    ],
  );

  const chartScales = useMemo(() => {
    // Collect all SoH values to determine the Y axis limits
    const allValues = [
      // Current vehicle points
      ...(data?.data_points?.length ? data.data_points.map((p) => p.soh) : []),
      // Initial curve points
      ...(initialCurvePoints?.map((p) => p.y) || []),
      // Prediction points
      ...(currentVehiclePredictionPoints?.map((p) => p.y) || []),
      // Trendline points
      ...(completeTrendlineMinPoints?.map((p) => p.y) || []),
      ...(completeTrendlineMaxPoints?.map((p) => p.y) || []),
      ...(completeTrendlineAvgPoints?.map((p) => p.y) || []),
    ];

    // Maximum value with a 5% margin above
    const maxValue = Math.ceil(Math.max(...allValues)) + 5;
    // Minimum value to ensure good visualization (with margin)
    const minValue = Math.floor(Math.min(...allValues) - 5);
    const range = maxValue - minValue;
    const stepSize = Math.ceil(range / 5); // 5 graduations for more precision

    // Maximum odometer for the X axis
    const maxOdometer = data?.data_points?.length
      ? data.data_points[data.data_points.length - 1].odometer + PREDICTION_DISTANCE
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
        min: 0, // Ensure the X axis starts at 0 km
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
    data,
    currentVehiclePredictionPoints,
    completeTrendlineMinPoints,
    completeTrendlineMaxPoints,
    completeTrendlineAvgPoints,
    initialCurvePoints,
  ]);

  if (!vin || isLoading) {
    return <LoadingSmall />;
  }

  if (!data || !data.data_points?.length) {
    return (
      <div className="h-full flex items-center justify-center">
        <p className="text-gray-blue italic">No data available</p>
      </div>
    );
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
