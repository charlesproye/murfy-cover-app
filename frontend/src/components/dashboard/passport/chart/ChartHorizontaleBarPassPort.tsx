// @ts-nocheck

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
  TooltipItem,
  Chart,
} from 'chart.js';
import { RangeBarChartProps } from '@/interfaces/passport/InfoVehicle';

ChartJS.register(CategoryScale, LinearScale, BarElement, Title, Tooltip, Legend);

const ChartHorizontalBarPassport: React.FC<RangeBarChartProps> = ({ data }) => {
  const labels = [
    `${data?.initial.soh}%`,
    `${data?.current.soh}%`,
    ...data.predictions.map((pred) => `${pred.soh}%`),
  ];

  const rangeData = [
    { min: data?.initial.range_min, max: data?.initial.range_max },
    { min: data?.current.range_min, max: data?.current.range_max },
    ...data.predictions.map((pred) => ({ min: pred.range_min, max: pred.range_max })),
  ];

  const chartData = {
    labels,
    datasets: [
      {
        data: rangeData.map((d) => d?.min),
        backgroundColor: '#007AFF',
        borderRadius: 10,
        borderWidth: 0,
        barThickness: 20,
        datalabels: {
          color: '#FFFFFF',
          formatter: (value: number) => `${value} km`,
          anchor: 'center',
          align: 'center',
        },
      },
      {
        data: rangeData.map((d) => d?.max - d?.min),
        backgroundColor: (context) => {
          const index = context.dataIndex;
          return index === 0 ? 'transparent' : '#a1c3fa';
        },
        borderRadius: 10,
        borderWidth: 0,
        barThickness: 20,
        datalabels: {
          color: '#FFFFFF',
          formatter: (value: number, context: { dataIndex: number }) => {
            const index = context.dataIndex;
            return index === 0 ? '' : `${rangeData[index].max} km`;
          },
          anchor: 'center',
          align: 'center',
        },
      },
    ],
  };

  const options = {
    indexAxis: 'y' as const,
    responsive: true,
    stacked: true,
    plugins: {
      legend: {
        display: false,
      },
      tooltip: {
        callbacks: {
          label: (context: TooltipItem<'bar'>) => {
            const datasetIndex = context.datasetIndex;
            const index = context.dataIndex;
            if (datasetIndex === 0) {
              return `Min Range: ${rangeData[index].min} km`;
            }
            return `Max Range: ${rangeData[index].max} km`;
          },
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
          callback: (value: number) => `${value} km`,
        },
        border: {
          display: false,
        },
        stacked: true,
        max: data.initial.range_min,
      },
      y: {
        grid: {
          display: false,
        },
        border: {
          display: false,
        },
        stacked: true,
      },
    },
    animation: {
      onComplete: function (this: Chart<'bar', number[], unknown>) {
        const chart = this;
        const ctx = chart.ctx;
        chart.data.datasets.forEach((dataset, datasetIndex: number) => {
          if (datasetIndex === 0) {
            const meta = chart.getDatasetMeta(datasetIndex);
            meta.data.forEach((bar, index: number) => {
              let text: string = '';
              switch (index) {
                case 0:
                  text = 'Initial range (WLTP)';
                  break;
                case 1:
                  text = 'Current remaining range';
                  break;
                case 2:
                  text = `End of contract (km)`;
                  break;
                case 3:
                  text = `End of warranty (km)`;
                  break;
              }

              ctx.fillStyle = '#FFFFFF';
              ctx.textAlign = 'left';
              ctx.textBaseline = 'middle';
              ctx.font = '12px Poppins';
              const x = chart.width / 12;
              const y = bar.y + 1.5;
              ctx.fillText(text, x, y);
            });
          }
        });
      },
      duration: 0,
    },

    maintainAspectRatio: false,
  };

  return <Bar data={chartData} options={options} />;
};
export default ChartHorizontalBarPassport;
