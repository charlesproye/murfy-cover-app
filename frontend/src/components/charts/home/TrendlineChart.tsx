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
import { getTrendlinePoints, parseTrendlineEquation, Point } from '@/lib/trendline';

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

    if (
      !brandEquations ||
      !brandEquations.trendline_bib ||
      !brandEquations.trendline_bib_max ||
      !brandEquations.trendline_bib_min
    ) {
      return null;
    }

    try {
      const avgEquation = brandEquations.trendline_bib;
      const maxEquation = brandEquations.trendline_bib_max;
      const minEquation = brandEquations.trendline_bib_min;

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

  const getPointColor = (item: TrendlineData): string => {
    switch (item.score) {
      case 'A':
        return 'rgba(35, 196, 94, 0.7)'; // Green for score A
      case 'B':
        return 'rgba(166, 230, 55, 0.7)'; // Lime for score B
      case 'C':
        return 'rgba(252, 222, 68, 0.7)'; // Yellow for score C
      case 'D':
        return 'rgba(250, 145, 60, 0.7)'; // Orange for score D
      case 'E':
        return 'rgba(240, 67, 70, 0.7)'; // Red for score E
      default:
        return 'rgba(0, 122, 255, 0.4)'; // Blue for other scores
    }
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
        borderColor: 'rgba(0, 200, 0, 0.9)',
        borderWidth: 2,
        borderDash: [5, 5],
        tension: 0.4,
        pointRadius: 0,
      },
      {
        type: 'line' as const,
        label: 'Trendline Avg',
        data: generateTrendlinePoints('avg'),
        borderColor: 'rgba(0, 122, 255, 0.9)',
        borderWidth: 2,
        borderDash: [5, 5],
        tension: 0.4,
        pointRadius: 0,
      },
      {
        type: 'line' as const,
        label: 'Trendline Min',
        data: generateTrendlinePoints('min'),
        borderColor: 'rgba(255, 0, 0, 0.9)',
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
