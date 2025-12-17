'use client';

import React from 'react';
import {
  Chart as ChartJS,
  ArcElement,
  Tooltip,
  Legend,
  ChartData,
  TooltipItem,
} from 'chart.js';
import { Doughnut } from 'react-chartjs-2';
import useGetChargingCycles from '@/hooks/dashboard/passport/useGetChargingCycles';
import LoadingSmall from '@/components/common/loading/loadingSmall';
import { IconBolt } from '@tabler/icons-react';

ChartJS.register(ArcElement, Tooltip, Legend);

const ChargingCyclesDonut: React.FC<{ formatDate: string; vin: string | undefined }> = ({
  formatDate,
  vin,
}) => {
  const { data, isLoading } = useGetChargingCycles(vin, formatDate);

  const chartData: ChartData<'doughnut'> = {
    labels: ['< 2.5 kW', '> 2.5 kW', 'Fast charging'],
    datasets: [
      {
        data: [
          data?.level_1_count || 0,
          data?.level_2_count || 0,
          data?.level_3_count || 0,
        ],
        backgroundColor: [
          '#4CAF50', // Vert pour level 1 (charge lente)
          '#FFC107', // Jaune pour level 2 (charge moyenne)
          '#F44336', // Rouge pour level 3 (charge rapide)
        ],
        borderWidth: 0,
      },
    ],
  };

  const options = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        display: false,
        position: 'bottom' as const,
        labels: {
          padding: 20,
          usePointStyle: true,
          pointStyle: 'circle',
        },
      },
      tooltip: {
        callbacks: {
          label: (context: TooltipItem<'doughnut'>) => {
            const label = context.label || '';
            return `${label}`;
          },
        },
      },
    },
    cutout: '70%',
  };

  if (!vin || isLoading) {
    return <LoadingSmall />;
  }

  return (
    <div className="bg-white w-full h-auto rounded-[20px] py-6 px-8 box-border">
      <div className="flex flex-col h-full">
        <div className="flex items-center gap-2 mb-6">
          <IconBolt className="text-primary" size={20} />
          <h3 className="text-primary font-medium">Charging Distribution</h3>
        </div>

        <div className="flex flex-col items-center flex-1">
          <div className="flex flex-wrap items-center justify-center gap-2 mb-6">
            <div className="flex items-center gap-1">
              <div className="bg-[#4CAF50] min-w-[12px] h-3 rounded-full"></div>
              <p className="text-gray-blue text-xs whitespace-nowrap">Slow (AC)</p>
            </div>
            <div className="flex items-center gap-1">
              <div className="bg-[#FFC107] min-w-[12px] h-3 rounded-full"></div>
              <p className="text-gray-blue text-xs whitespace-nowrap">Medium (AC)</p>
            </div>
            <div className="flex items-center gap-1">
              <div className="bg-[#F44336] min-w-[12px] h-3 rounded-full"></div>
              <p className="text-gray-blue text-xs whitespace-nowrap">
                Fast charging (DC)
              </p>
            </div>
          </div>

          <div className="h-[120px] w-[120px] relative">
            <Doughnut data={chartData} options={options} />
          </div>
        </div>
      </div>
    </div>
  );
};

export default ChargingCyclesDonut;
