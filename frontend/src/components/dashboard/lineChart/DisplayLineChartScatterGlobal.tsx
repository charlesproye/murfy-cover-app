import React from 'react';
import { FleetBrandData } from '@/interfaces/dashboard/global/GlobalDashboard';

import LineChartScatterGlobal from '../chart/LineChartScatterGlobal';

const DisplayLineChartScatterGlobal: React.FC<{ data: FleetBrandData }> = ({ data }) => {
  return (
    <div className="w-full h-[435px] min-h-[250px]">
      <LineChartScatterGlobal data={data} />
      <p className="text-xs text-gray-500 text-center italic">{'Mileage (km)'}</p>
    </div>
  );
};

export default DisplayLineChartScatterGlobal;
