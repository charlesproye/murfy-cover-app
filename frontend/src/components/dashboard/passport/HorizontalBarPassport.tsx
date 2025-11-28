// @ts-nocheck

import React from 'react';
import ChartHorizontalBarPassport from './chart/ChartHorizontaleBarPassPort';

import { RangeBarChartProps } from '@/interfaces/passport/InfoVehicle';
import TitleBox from '@/components/common/TitleBox';

const HorizontalBarPassport: React.FC<RangeBarChartProps> = ({ data }) => {
  return (
    <div style={{ width: '100%', height: '300px' }}>
      <div className="flex flex-row justify-between">
        <TitleBox title={'Estimated Range'} titleSecondary={'Report and Forecast'} />
        <div className="flex gap-4">
          <span className="flex items-center gap-2">
            <div className="w-2 h-2 rounded-full bg-[#007AFF]"></div>
            <p className="text-gray-blue text-sm">Minimum Range</p>
          </span>
          <span className="flex items-center gap-2">
            <div className="w-2 h-2 rounded-full bg-blue-less-filled"></div>
            <p className="text-gray-blue text-sm">Estimated Range</p>
          </span>
        </div>
      </div>
      <ChartHorizontalBarPassport data={data} />
      <p className="text-xs text-gray-blue text-center italic">Range (km)</p>
    </div>
  );
};
export default HorizontalBarPassport;
