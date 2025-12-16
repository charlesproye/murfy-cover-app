'use client';

import React from 'react';
import LineChart from '@/components/dashboard/chart/LineChart';

import { useParams } from 'next/navigation';
import VehicleInfo from '@/components/dashboard/passport/VehicleInfo';
import EstimatedRange from '@/components/dashboard/passport/EstimatedRange';
import KpiAdditional from '@/components/dashboard/passport/KpiAdditional';
import TitleBox from '@/components/common/TitleBox';

const PassPort: React.FC = () => {
  const params = useParams();
  const vin = params.vin as string;

  return (
    <div className="flex flex-col h-full w-full space-y-8 pt-2">
      <VehicleInfo vin={vin} />

      <div className="w-full h-[500px] bg-white rounded-[20px] p-4 pb-12 px-8 box-border">
        <div className="flex justify-between items-center mb-4">
          <TitleBox title={'Vehicle overview'} titleSecondary={'SoH evolution'} />
          <div className="flex gap-6">
            <span className="flex items-center gap-2">
              <div className="w-2 h-2 rounded-full bg-blue-almost-filled"></div>
              <p className="text-gray-blue text-sm mr-2">Vehicle SoH</p>
              <div className="w-4 h-[2px] border-t-2 border-dashed border-blue-almost-filled"></div>
              <p className="text-gray-blue text-sm">Prediction</p>
            </span>
            <span className="flex items-center gap-2">
              <div className="w-4 h-[2px] border-t-2 border-dashed border-[#9BA3AF]"></div>
              <p className="text text-sm text-gray-blue">Average Model SoH</p>
            </span>
          </div>
        </div>
        <div className="h-[calc(100%-60px)]">
          <LineChart vin={vin} />
        </div>
      </div>

      <div className="flex flex-col lg:flex-col gap-4 mt-4">
        <div className="w-full lg:w-full">
          <KpiAdditional vin={vin} />
        </div>
        <div className="w-full lg:w-full lg:mt-0 mb-16">
          <EstimatedRange vin={vin} />
        </div>
      </div>
    </div>
  );
};

export default PassPort;
