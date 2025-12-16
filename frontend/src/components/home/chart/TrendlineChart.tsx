import React, { memo, useMemo } from 'react';
import {
  Chart as ChartJS,
  LinearScale,
  PointElement,
  LineElement,
  Tooltip,
  Legend,
  TooltipItem,
  ChartEvent,
  ActiveElement,
  ChartData,
} from 'chart.js';
import { Scatter } from 'react-chartjs-2';
import {
  TrendlineChartProps,
  TrendlineData,
} from '@/interfaces/dashboard/home/ResponseApi';
import { useRouter } from 'next/navigation';
import {
  calculateLogarithmicRegression,
  getTrendlinePoints,
  parseTrendlineEquation,
  Point,
} from '@/utils/regression';

ChartJS.register(LinearScale, PointElement, LineElement, Tooltip, Legend);

const TrendlineChart: React.FC<TrendlineChartProps> = ({
  data,
  selectedBrand,
  trendlineEquations,
}) => {
  const router = useRouter();
  const coefficients = useMemo(() => {
    if (!trendlineEquations || !selectedBrand) {
      return null;
    }

    // Find the equations for the selected brand
    const brandEquations = trendlineEquations.find(
      (eq) => eq.brand.oem_id === selectedBrand,
    );

    if (!brandEquations) {
      return null;
    }

    try {
      const avgEquation = JSON.parse(brandEquations.trendline).trendline;
      const maxEquation = JSON.parse(brandEquations.trendline_max).trendline;
      const minEquation = JSON.parse(brandEquations.trendline_min).trendline;

      const parsedCoefficients = {
        avg: parseTrendlineEquation(avgEquation),
        max: parseTrendlineEquation(maxEquation),
        min: parseTrendlineEquation(minEquation),
      };

      return parsedCoefficients;
    } catch (error) {
      console.error('Error parsing the equations:', error);
      return null;
    }
  }, [selectedBrand, trendlineEquations]);

  const regressionCalculator = useMemo(() => {
    if (!data.length || !coefficients) {
      return null;
    }

    const points = data
      .filter((item) => item.in_fleet)
      .map((item) => ({
        x: item.odometer,
        y: item.soh,
      }));

    const result = calculateLogarithmicRegression(
      points,
      undefined,
      undefined,
      true,
      coefficients.avg,
    );

    return Array.isArray(result) ? null : result;
  }, [data, coefficients]);

  const getPointColor = (item: TrendlineData): string => {
    if (item.score === 'A') {
      return 'rgba(0, 200, 0, 0.8)'; // Green for score A
    }
    if (item.score === 'F') {
      return 'rgba(255, 0, 0, 0.8)'; // Red for score F
    }

    // For other scores, use a blue with a variable opacity
    const maxDistance = 10;
    if (!regressionCalculator) return 'rgba(0, 122, 255, 0.5)';

    const expectedY = regressionCalculator.getTrendlineY(item.odometer);
    const distance = item.soh - expectedY;
    const opacity = Math.max(0.2, 1 - Math.abs(distance) / maxDistance);
    return `rgba(0, 122, 255, ${opacity})`;
  };

  // Generate the trendline points for a given curve
  const generateTrendlinePoints = (type: 'min' | 'max' | 'avg'): Point[] => {
    if (!data.length || !coefficients) {
      return [];
    }

    const minX = 0;
    const maxX = Math.max(...data.map((item) => item.odometer));

    const points = getTrendlinePoints(minX, maxX, coefficients[type]);

    return points;
  };

  const chartData = {
    datasets: [
      {
        type: 'line' as const,
        label: 'Trendline Max',
        data: generateTrendlinePoints('max'),
        borderColor: 'rgba(0, 200, 0, 0.7)',
        borderWidth: 2,
        borderDash: [5, 5],
        tension: 0.4,
        pointRadius: 0,
      },
      {
        type: 'line' as const,
        label: 'Trendline Avg',
        data: generateTrendlinePoints('avg'),
        borderColor: 'rgba(0, 122, 255, 0.7)',
        borderWidth: 2,
        borderDash: [5, 5],
        tension: 0.4,
        pointRadius: 0,
      },
      {
        type: 'line' as const,
        label: 'Trendline Min',
        data: generateTrendlinePoints('min'),
        borderColor: 'rgba(255, 0, 0, 0.7)',
        borderWidth: 2,
        borderDash: [5, 5],
        tension: 0.4,
        pointRadius: 0,
      },
      {
        type: 'scatter' as const,
        label: 'Vehicles',
        data: data
          .filter((item) => item.in_fleet)
          .map((item) => ({
            x: item.odometer,
            y: item.soh,
            vin: item.vin,
          })),
        backgroundColor: data
          .filter((item) => item.in_fleet)
          .map((item) => getPointColor(item)),
        borderColor: 'transparent',
        pointRadius: 4,
        pointHoverRadius: 6,
      },
    ],
  };

  const options = {
    onClick: (_: ChartEvent, elements: ActiveElement[]) => {
      if (!elements || elements.length === 0) return;

      const element = elements[0];
      if (element.datasetIndex !== 3) return; // Update to reflect the new index of the scatter dataset

      const datasetData = chartData.datasets[3]?.data;
      if (!datasetData) return;

      const dataIndex = element.index;
      if (typeof dataIndex !== 'number' || dataIndex >= datasetData.length) return;

      const point = datasetData[dataIndex] as { x: number; y: number; vin: string };
      if (!point?.vin) return;

      router.push(`/dashboard/passport/${point.vin}`);
    },
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        display: false,
      },
      tooltip: {
        callbacks: {
          label: (context: TooltipItem<'scatter'>) => {
            // Gestion spéciale pour les points de véhicules (scatter)
            if (context.datasetIndex === 3) {
              const dataPoint = context.raw as { x: number; y: number; vin: string };
              return ` VIN: ${dataPoint.vin}, SoH: ${dataPoint.y.toFixed(1)}%, Odometer: ${dataPoint.x.toFixed(0)}km`;
            }
            // Gestion pour les points de trendline
            return ` ${context.dataset.label}: ${context.parsed.y.toFixed(1)}% @ ${context.parsed.x.toFixed(0)}km`;
          },
        },
      },
      datalabels: {
        display: false,
      },
    },
    scales: {
      x: {
        type: 'linear' as const,
        position: 'bottom' as const,
        title: {
          display: false,
        },
        grid: {
          color: 'transparent',
        },
        min: 0,
        max:
          Math.ceil((Math.max(...data.map((item) => item.odometer)) * 1.0) / 10000) *
          10000,
        ticks: {
          maxTicksLimit: 10,
        },
      },
      y: {
        title: {
          display: false,
        },
        grid: {
          color: 'transparent',
        },
        max: 110,
        min: 80,
        ticks: {
          callback: function (value: number | string) {
            return value + '%';
          },
          max: 100,
          includeBounds: false,
        },
      },
    },
  };

  // If no coefficients, display only the points
  if (!coefficients) {
    const chartDataNoCoefficients = {
      datasets: [
        {
          type: 'scatter' as const,
          label: 'Vehicles',
          data: data
            .filter((item) => item.in_fleet)
            .map((item) => ({
              x: item.odometer,
              y: item.soh,
              vin: item.vin,
            })),
          backgroundColor: data
            .filter((item) => item.in_fleet)
            .map((item) => getPointColor(item)),
          borderColor: 'transparent',
          pointRadius: 4,
          pointHoverRadius: 6,
        },
      ],
    };

    const optionsNoCoefficients = {
      ...options,
      onClick: (_: ChartEvent, elements: ActiveElement[]) => {
        if (!elements || elements.length === 0) return;

        const point = chartDataNoCoefficients.datasets[0].data.find(
          (_, index) => index === elements[0].index,
        );
        if (!point) return;

        router.push(`/dashboard/passport/${point.vin}`);
      },
    };

    return (
      <Scatter
        data={chartDataNoCoefficients as ChartData<'scatter'>}
        options={optionsNoCoefficients}
      />
    );
  }

  return <Scatter data={chartData as ChartData<'scatter'>} options={options} />;
};

export default memo(TrendlineChart);
