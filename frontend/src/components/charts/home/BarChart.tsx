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
  ChartEvent,
  InteractionItem,
} from 'chart.js';
import { RangeSoh } from '@/interfaces/dashboard/home/response';

ChartJS.register(CategoryScale, LinearScale, BarElement, Title, Tooltip, Legend);

type BarChartProps = {
  data: RangeSoh[];
  type: 'SoH' | 'Mileage';
  setIsDisplay: (isDisplay: boolean, clickedData: string | null) => void;
};

const BarChart: React.FC<BarChartProps> = ({ data, type, setIsDisplay }) => {
  const labels = data.map((item) => {
    if (type === 'SoH') {
      return item.lower_bound === 0
        ? `< ${item.upper_bound}%`
        : `${item.upper_bound}-${item.lower_bound}%`;
    } else {
      const lowerBound =
        item.lower_bound >= 1000
          ? `${Math.round(item.lower_bound / 1000)}k`
          : item.lower_bound;
      const upperBound =
        item.upper_bound >= 1000
          ? `${Math.round(item.upper_bound / 1000)}k`
          : item.upper_bound;
      return `${lowerBound}-${upperBound}`;
    }
  });
  const vehicleCounts = data.map((item) => item.vehicle_count);

  const chartData = {
    labels,
    datasets: [
      {
        label: 'Vehicle Count',
        data: vehicleCounts,
        backgroundColor: '#007AFF',
        borderColor: '#007AFF',
        borderWidth: 2,
        borderRadius: 10,
      },
    ],
  };

  const options = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        display: false, // Hide the legend
      },
      title: {
        display: false, // Hide the title
      },
      datalabels: {
        display: false,
      },
    },

    scales: {
      y: {
        beginAtZero: true,
        grid: {
          color: 'transparent', // Remove the grid lines for the y-axis
        },
      },
      x: {
        grid: {
          color: 'transparent', // Remove the grid lines for the x-axis
        },
        // Adjust bar width
        ticks: {
          autoSkip: false, // Show all labels
        },
      },
    },
    elements: {
      bar: {
        borderWidth: 2, // Thicker border for bars
        borderRadius: 10, // Rounded corners for bars
      },
    },
    // Adjust bar thickness
    barPercentage: 0.9, // Adjust this value to make bars thinner (0.0 to 1.0)
    categoryPercentage: 0.5, // Adjust this value to control the space between bars

    onClick: (e: ChartEvent, elements: InteractionItem[]) => {
      if (elements && elements.length > 0 && elements[0]?.index !== undefined) {
        const elementIndex = elements[0].index;
        const label = labels[elementIndex];

        if (label) {
          setIsDisplay(true, label);
        }
      }
    },
  };

  return <Bar data={chartData} options={options} />;
};

export default BarChart;
