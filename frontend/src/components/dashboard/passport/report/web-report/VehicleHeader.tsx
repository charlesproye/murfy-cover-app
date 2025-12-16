import React from 'react';
import ScoreCircle from '../ScoreCircle';
import { InfoVehicleResult } from '@/interfaces/dashboard/passport/infoVehicle';
import { Score } from '@/interfaces/dashboard/passport/ScoreCard/ScoreCardProps';
import Image from 'next/image';
import { IconBatteryCharging } from '@tabler/icons-react';
import { DEFAULT_CAR_IMAGE } from '@/lib/utils';

interface VehicleHeaderProps {
  brand: string;
  model: string;
  vin: string;
  immatriculation: string;
  mileage: number | string;
  image: string;
  score: Score;
  start_date: string;
  vehicleBatteryInfo: InfoVehicleResult;
}

const VehicleHeader: React.FC<VehicleHeaderProps> = ({
  brand,
  model,
  vin,
  mileage,
  immatriculation,
  image,
  start_date,
  vehicleBatteryInfo,
  score,
}) => {
  return (
    <div className="flex flex-col md:flex-row bg-white rounded-t-lg shadow-xs overflow-hidden mb-6 w-full">
      <div className="flex-1 p-4 md:p-6 bg-linear-to-b from-green-rapport-extra-light/15 via-green-rapport/5 to-green-rapport/0">
        <div className="flex justify-center items-center gap-x-4">
          <div className="w-1/5 flex items-center h-full justify-center">
            <div className="bg-white p-2 rounded-lg border border-gray/20 shadow-xs w-[140px] h-[80px] flex items-center justify-center overflow-hidden">
              <Image
                src={image || DEFAULT_CAR_IMAGE}
                alt={`${brand} ${model}`}
                width={140}
                height={80}
                className="object-contain w-full h-full"
                onError={(e) => {
                  const target = e.target as HTMLImageElement;
                  target.src = DEFAULT_CAR_IMAGE;
                }}
              />
            </div>
          </div>

          <div className="w-4/5 ml-4 flex flex-col justify-center gap-y-2">
            <div className="flex flex-row items-center gap-2 max-w-full">
              <h2 className="text-lg font-semibold uppercase truncate max-w-[180px]">
                {brand}
              </h2>
              <h3 className="text-lg font-semibold truncate max-w-[260px]">{model}</h3>
            </div>
            <div className="grid grid-cols-2 gap-x-8 gap-y-2 text-xs mt-1">
              <div>
                <p className="text-gray/50 font-semibold text-xs">VIN</p>
                <p className="font-medium truncate max-w-[120px]">{vin}</p>
              </div>
              <div>
                <p className="text-gray/50 font-semibold text-xs">IMMATRICULATION</p>
                <p className="font-medium truncate max-w-[90px]">{immatriculation}</p>
              </div>
              <div>
                <p className="text-gray/50 font-semibold text-xs">MILEAGE</p>
                <p className="font-medium truncate">{mileage} KM</p>
              </div>
              <div>
                <p className="text-gray/50 font-semibold text-xs">REGISTRATION DATE</p>
                <p className="font-medium truncate">{start_date}</p>
              </div>
            </div>
          </div>
        </div>
        <div className="flex items-center gap-2 mb-2 mt-8">
          <IconBatteryCharging className="w-5 h-5 text-green-rapport" />
          <h4 className="text-md font-medium text-black">Battery</h4>
        </div>
        <div className="flex flex-row flex-wrap justify-between gap-4">
          <div>
            <p className="text-gray/50 text-xs font-semibold">BATTERY OEM</p>
            <p className="font-medium text-xs truncate">
              {vehicleBatteryInfo?.battery_info?.oem || ''}
            </p>
          </div>
          <div>
            <p className="text-gray/50 text-xs font-semibold">CHEMISTRY</p>
            <p className="font-medium text-xs truncate">
              {vehicleBatteryInfo?.battery_info?.chemistry || ''}
            </p>
          </div>
          <div>
            <p className="text-gray/50 text-xs font-semibold">CAPACITY</p>
            <p className="font-medium text-xs truncate">
              {vehicleBatteryInfo?.battery_info?.capacity === 0
                ? ''
                : `${vehicleBatteryInfo?.battery_info?.capacity} kWh`}
            </p>
          </div>
          <div>
            <p className="text-gray/50 text-xs font-semibold">WLTP RANGE</p>
            <p className="font-medium text-xs truncate">
              {vehicleBatteryInfo?.battery_info?.range === 0
                ? ''
                : `${vehicleBatteryInfo?.battery_info?.range} km`}
            </p>
          </div>
          <div>
            <p className="text-gray/50 text-xs font-semibold">CONSUMPTION</p>
            <p className="font-medium text-xs truncate">
              {vehicleBatteryInfo?.battery_info?.consumption === 0
                ? ''
                : `${vehicleBatteryInfo?.battery_info?.consumption} kWh/100km`}
            </p>
          </div>
        </div>
      </div>

      <div className="p-2 md:p-2 flex flex-col items-center border-l border-green-rapport/10 justify-center w-full md:w-1/3 bg-linear-to-b from-green-rapport-extra-light/15 via-green-rapport/5 to-green-rapport/0 ">
        <ScoreCircle
          vehicle_battery_info={vehicleBatteryInfo}
          displayScale={true}
          score={score}
        />
      </div>
    </div>
  );
};

export default VehicleHeader;
