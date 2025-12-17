import React from 'react';
import { FleetBrandData } from '@/interfaces/dashboard/global/GlobalDashboard';
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
} from 'chart.js';
import { Line } from 'react-chartjs-2';
import { calculateLogarithmicRegression } from '@/lib/trendline';

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
);

const colors = ['#2d67f6', '#34c759', '#ff9500', '#5856d6', '#007aff'];

const LineChartScatterGlobal: React.FC<{ data: FleetBrandData }> = ({ data }) => {
  if (!data || Object.keys(data).length === 0) {
    return <p>No data available</p>;
  }

  const allXValues = Object.values(data).flatMap((brands) =>
    Object.values(brands).flatMap((items) => items.map((item) => item.odometer)),
  );
  const globalMinX = Math.min(...allXValues);
  const globalMaxX = Math.max(...allXValues);

  const datasets = Object.entries(data).flatMap(([fleetName, brands]) =>
    Object.entries(brands)
      .map(([brandName, items], brandIndex, brandsArray) => {
        // Filtrer les points qui ont un SOH null
        const validItems = items.filter((item) => item.soh !== null);

        // Si pas de données valides ou pas assez de points, on ne crée pas de dataset
        if (validItems.length === 0) {
          return null;
        }

        const colorIndex =
          (Object.keys(data).indexOf(fleetName) * brandsArray.length + brandIndex) %
          colors.length;

        const points = validItems.map(
          (item) => [item.odometer, item.soh] as [number, number],
        );
        const trendlineData = calculateLogarithmicRegression(
          points,
          globalMinX,
          globalMaxX,
        );

        // Si pas de points de tendance (pas assez de points valides), ne pas créer de dataset
        if (!Array.isArray(trendlineData) || trendlineData.length === 0) {
          return null;
        }

        return {
          label: `${fleetName} - ${brandName.charAt(0).toUpperCase() + brandName.slice(1)}`,
          data: trendlineData,
          borderColor: colors[colorIndex],
          backgroundColor: colors[colorIndex],
          tension: 0.4,
          fill: false,
          pointStyle: 'circle',
          pointRadius: 0,
          borderWidth: 2,
        };
      })
      .filter((dataset): dataset is NonNullable<typeof dataset> => dataset !== null),
  );

  const allSohValues = datasets.flatMap((dataset) =>
    dataset.data.map((point) => point.y),
  );
  const maxValue = Math.min(100, Math.max(...allSohValues));
  const minValue = Math.max(0, Math.floor(Math.min(...allSohValues) - 5));

  const range = maxValue - minValue;
  const stepSize = Math.ceil(range / 5);

  const minShow = parseFloat((minValue - stepSize).toFixed(2));
  const maxShow = parseFloat((maxValue + stepSize).toFixed(2));

  const scatterData = {
    datasets,
  };

  const options = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      title: { display: false },
      legend: {
        display: true,
        position: 'top' as const,
        labels: {
          padding: 20,
          usePointStyle: true,
          pointStyle: 'line',
          font: { size: 12 },
        },
      },
      tooltip: {
        enabled: true,
        backgroundColor: 'rgba(0, 0, 0, 0.8)',
        callbacks: {
          label: (context: TooltipItem<'line'>) => {
            return `${context.dataset.label} - SOH: ${context.parsed.y.toFixed(1)}%, Odometer: ${context.parsed.x.toFixed(0)}km`;
          },
        },
      },
    },
    scales: {
      x: {
        type: 'linear' as const,
        position: 'bottom' as const,
        ticks: {
          font: { size: 12 },
          maxTicksLimit: 10,
          stepSize: 10000,
        },
        grid: { display: false, color: 'transparent' },
        border: { display: false },
        min:
          Math.floor(
            (Math.min(...datasets.flatMap((d) => d.data.map((p) => p.x))) * 0.95) / 10000,
          ) * 10000,
        max:
          Math.ceil(
            (Math.max(...datasets.flatMap((d) => d.data.map((p) => p.x))) * 1.0) / 10000,
          ) * 10000,
      },
      y: {
        beginAtZero: false,
        ticks: {
          stepSize: stepSize,
          font: { size: 12 },
          callback: function (tickValue: number | string) {
            return `${Math.round(Number(tickValue))}%`;
          },
        },
        grid: { drawOnChartArea: true, color: 'transparent' },
        min: minShow,
        max: Math.min(100, maxShow),
        border: { display: false },
      },
    },
  };

  return <Line className="w-full " data={scatterData} options={options} />;
};

export default LineChartScatterGlobal;
