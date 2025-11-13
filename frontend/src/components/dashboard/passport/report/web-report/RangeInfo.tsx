import React from 'react';
import {
  IconCircleCheck,
  IconBuilding,
  IconRoad,
  IconArrowsShuffle,
} from '@tabler/icons-react';
import { InfoVehicleResult } from '@/interfaces/dashboard/passport/infoVehicle';

interface RangeInfoProps {
  reportData: InfoVehicleResult;
}

const RangeInfo: React.FC<RangeInfoProps> = ({ reportData }) => {
  const URBAN_SUMMER_COEF = 1;
  const URBAN_WINTER_COEF = 1.5;

  const MOTORWAY_SUMMER_COEF = 1.55;
  const MOTORWAY_WINTER_COEF = 2;

  const MIXED_SUMMER_COEF = 1.25;
  const MIXED_WINTER_COEF = 1.75;

  const formatRange = (range: number | undefined, coefficient: number): string => {
    if (range === undefined || range === null || range === 0) {
      return '-';
    }
    return `${Math.round(range / coefficient)} km`;
  };

  const calculateUrbanSummerRange = (range: number | undefined): string => {
    return formatRange(range, URBAN_SUMMER_COEF);
  };

  const calculateMotorwaySummerRange = (range: number | undefined): string => {
    return formatRange(range, MOTORWAY_SUMMER_COEF);
  };

  const calculateMixedSummerRange = (range: number | undefined): string => {
    return formatRange(range, MIXED_SUMMER_COEF);
  };

  const calculateUrbanWinterRange = (range: number | undefined): string => {
    return formatRange(range, URBAN_WINTER_COEF);
  };

  const calculateMotorwayWinterRange = (range: number | undefined): string => {
    return formatRange(range, MOTORWAY_WINTER_COEF);
  };

  const calculateMixedWinterRange = (range: number | undefined): string => {
    return formatRange(range, MIXED_WINTER_COEF);
  };

  return (
    <div className="w-full md:w-1/2 bg-white rounded-xl p-4 md:p-6 shadow-sm">
      <div className="flex items-center space-x-2 mb-4">
        <div className="w-6 h-6 rounded-full bg-green-100 flex items-center justify-center">
          <IconCircleCheck size={24} className="text-green-rapport" />
        </div>
        <h3 className="font-medium">Range</h3>
      </div>

      <p className="text-xs mb-4">
        Bib compares the WLTP range announced by the manufacturer at the factory with the
        current range observed during the vehicle's monitoring by Bib Batteries.
      </p>

      <div className="mt-6">
        <div className="grid grid-cols-3 mb-3 border-b border-gray/20 pb-4">
          <div className="text-xs text-gray/70 font-medium">USAGE</div>
          <div className="text-xs text-gray/70 font-medium text-center">
            SUMMER (23°C)
          </div>
          <div className="text-xs text-gray/70 font-medium text-center">WINTER (0°C)</div>
        </div>

        <div className="grid grid-cols-3 items-center pb-3 border-b border-gray/20">
          <div className="flex items-center">
            <IconBuilding size={20} className="mr-2 text-green-rapport" />
            <span className="text-xs ">Urban</span>
          </div>
          <div className="text-xs text-center">
            {calculateUrbanSummerRange(reportData.battery_info?.range)}
          </div>
          <div className="text-xs text-center">
            {calculateUrbanWinterRange(reportData.battery_info?.range)}
          </div>
        </div>

        <div className="grid grid-cols-3 items-center py-3 border-b border-gray/20">
          <div className="flex items-center">
            <IconRoad size={20} className="mr-2 text-green-rapport" />
            <span className="text-xs">Motorway</span>
          </div>
          <div className="text-xs text-center">
            {calculateMotorwaySummerRange(reportData.battery_info?.range)}
          </div>
          <div className="text-xs text-center">
            {calculateMotorwayWinterRange(reportData.battery_info?.range)}
          </div>
        </div>

        <div className="grid grid-cols-3 items-center py-3">
          <div className="flex items-center">
            <IconArrowsShuffle size={20} className="mr-2 text-green-rapport" />
            <span className="text-xs">Mixed used</span>
          </div>
          <div className="text-xs text-center">
            {calculateMixedSummerRange(reportData.battery_info?.range)}
          </div>
          <div className="text-xs text-center">
            {calculateMixedWinterRange(reportData.battery_info?.range)}
          </div>
        </div>
      </div>
    </div>
  );
};

export default RangeInfo;
