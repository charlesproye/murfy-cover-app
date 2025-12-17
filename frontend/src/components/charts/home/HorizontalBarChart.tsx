import React from 'react';
import { Bar } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  Title,
  Tooltip,
  Legend,
  ScriptableContext,
  TooltipItem,
} from 'chart.js';
import { HorizontalBarChartProps } from '@/interfaces/dashboard/home/BarChartProps';

ChartJS.register(CategoryScale, LinearScale, BarElement, Title, Tooltip, Legend);

const HorizontalBarChart: React.FC<HorizontalBarChartProps> = ({ data, period }) => {
  const labels = data.map((item) => {
    const date = new Date(item.period);
    return period === 'Monthly'
      ? date.toLocaleDateString('en-US', { month: 'short' })
      : date.toLocaleDateString('en-US', { year: 'numeric' });
  });
  const values = data.map((item) => item.vehicle_count);

  const chartData = {
    labels,
    datasets: [
      {
        data: values,
        label: 'Vehicle Count',
        backgroundColor: (context: ScriptableContext<'bar'>) => {
          const maxIndex = values.indexOf(Math.max(...values));
          return context.dataIndex === maxIndex ? '#007AFF' : '#9BA3AF';
        },
        borderRadius: 10,
        borderWidth: 0,
        color: '#fff',
        barThickness: 20,
      },
    ],
  };

  const options = {
    indexAxis: 'y' as const,
    responsive: true,
    plugins: {
      legend: {
        display: false,
      },
      title: {
        display: false,
      },
      tooltip: {
        callbacks: {
          label: (context: TooltipItem<'bar'>) => `${context.parsed.x}`,
        },
        datalabels: {
          display: false,
        },
      },
    },
    scales: {
      x: {
        grid: {
          display: true,
          color: 'transparent',
          drawBorder: false,
        },
        ticks: {
          count: 5,
          stepSize: Math.ceil(Math.max(...values) / 4),
          max: Math.ceil(Math.max(...values)),
        },
        border: {
          display: false,
        },
      },
      y: {
        grid: {
          display: false,
          color: 'transparent',
        },
        border: {
          display: false,
        },
      },
    },
    maintainAspectRatio: false,
  };

  return <Bar data={chartData} options={options} />;
};

export default HorizontalBarChart;
