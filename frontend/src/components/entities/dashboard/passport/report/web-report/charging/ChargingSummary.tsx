import React, { useEffect, useState } from 'react';
import { InfoVehicleResult } from '@/interfaces/dashboard/passport/infoVehicle';
import { IconRecharging } from '@tabler/icons-react';
import {
  CHARGING_DATA,
  getChargeDuration,
  getChargeDurationValue,
  getCostForType,
} from '@/components/entities/dashboard/passport/report/web-report/charging/lib';

interface ChargingSummaryProps {
  reportData: InfoVehicleResult;
}

const ChargingSummary: React.FC<ChargingSummaryProps> = ({ reportData }) => {
  const [timePercentages, setTimePercentages] = useState<number[]>([]);
  const vehicleHasData = !!reportData.vehicle_info;

  useEffect(() => {
    if (!reportData.battery_info?.capacity) return;
    const timeValues = CHARGING_DATA.map((item) =>
      getChargeDurationValue(item.power.value, reportData.battery_info?.capacity),
    );
    const maxTime = Math.max(...timeValues);
    const normalizedTimes = timeValues.map((value) => (value / maxTime) * 100);
    setTimePercentages(normalizedTimes);
  }, [reportData.battery_info?.capacity]);

  return (
    <div className="rounded-xl shadow-xs max-w-4xl w-full overflow-hidden bg-white">
      <div className="w-full p-4 md:p-6">
        <div className="flex items-center justify-start space-x-2 mb-5">
          <div className="w-6 h-6 rounded-full flex items-center justify-center">
            <IconRecharging stroke={2} className="text-green-rapport" />
          </div>
          <h3 className="text-base font-medium">Charging Summary</h3>
        </div>
        <div className="flex justify-center">
          <table className="w-full h-fit mt-4 text-sm">
            <thead>
              <tr className="border-b border-gray/20">
                <th className="text-center py-2 font-normal text-gray text-xs">TYPE</th>
                <th className="text-center py-2 font-normal text-gray text-xs">
                  TIME AND COST OF CHARGE (10% - 80%)
                </th>
                <th className="text-center py-2 font-normal text-gray text-xs">
                  TYPICAL
                </th>
                <th className="text-center py-2 font-normal text-gray text-xs">POWER</th>
              </tr>
            </thead>
            <tbody>
              {CHARGING_DATA.map((item, index) => (
                <tr key={index} className="text-center text-xs h-16">
                  <td className="align-middle font-medium">{item.type}</td>
                  <td className="align-middle min-w-[280px]">
                    <div className="flex flex-col items-center justify-center w-full">
                      <div className="flex flex-row justify-between w-72 mb-1">
                        <span className="text-[13px] text-gray-700 font-semibold">
                          {getChargeDuration(
                            item.power.value,
                            reportData.battery_info?.capacity,
                          )}
                        </span>
                        <span className="text-[13px] text-gray-700 font-semibold">
                          {getCostForType(item.type, reportData.battery_info?.capacity)}
                        </span>
                      </div>
                      <div className="w-72 h-2 bg-gray/30 rounded-full overflow-hidden">
                        <div
                          className="h-full transition-all duration-300 ease-in-out"
                          style={{
                            width: `${timePercentages[index] || 0}%`,
                            backgroundColor: item.color,
                          }}
                        ></div>
                      </div>
                    </div>
                  </td>
                  <td className="align-middle">{item.outlet}</td>
                  <td className="align-middle">{item.power.label}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div className="w-full mt-4 overflow-hidden border border-green-rapport-light rounded-lg shadow-xs flex flex-col md:flex-row">
          <div className="bg-green-rapport-light/10 px-4 py-3 w-full flex flex-col justify-center">
            <p className="text-xs text-gray">
              {vehicleHasData
                ? `These data represent the different types of charge available for the vehicle ${reportData.vehicle_info?.brand || ''} ${reportData.vehicle_info?.model || ''}, classified by power and current type. Time and cost for a charge from 10% to 80%.`
                : 'These data represent the different types of charge available for your electric vehicle, classified by power and current type. Time and cost for a charge from 10% to 80%.'}
            </p>
          </div>
        </div>
      </div>
    </div>
  );
};

export default ChargingSummary;
