import React from 'react';
import { IconShieldCheck } from '@tabler/icons-react';
import { InfoVehicleResult } from '@/interfaces/dashboard/passport/infoVehicle';
import { formatNumber } from '@/lib/dataDisplay';
import { NO_DATA } from '@/lib/staticData';

interface WarrantyInfoProps {
  vehicleBatteryInfo: InfoVehicleResult;
}

const WarrantyInfo: React.FC<WarrantyInfoProps> = ({ vehicleBatteryInfo }) => {
  // Calcul du kilométrage restant pour la garantie
  const getWarrantyRemaining = (data: InfoVehicleResult): string => {
    const warrantyKm = data.vehicle_info?.warranty_km || 0;
    const currentMileage = data.vehicle_info?.odometer || 0;
    const remainingMileage = Math.max(0, warrantyKm - currentMileage);
    return formatNumber(remainingMileage, 'KM');
  };

  const calculateWarrantyKmPercentage = (data: InfoVehicleResult): number => {
    const warrantyKm = data.vehicle_info?.warranty_km || 0;
    const currentMileage = data.vehicle_info?.odometer || 0;
    if (warrantyKm <= 0) return 0;
    const percentage = ((warrantyKm - currentMileage) / warrantyKm) * 100;
    return Math.max(0, Math.min(100, percentage));
  };

  // Calcul du temps restant pour la garantie
  const getWarrantyTimeRemaining = (data: InfoVehicleResult): string => {
    const startDateStr = data.vehicle_info?.start_date;
    const warrantyYears = data.vehicle_info?.warranty_date;

    // Vérifier que les données nécessaires sont disponibles
    if (!startDateStr || warrantyYears === undefined || warrantyYears === null) {
      return NO_DATA;
    }

    const startDate = new Date(startDateStr);

    // Vérifier que la date est valide
    if (isNaN(startDate.getTime())) {
      return NO_DATA;
    }

    const endDate = new Date(startDate);
    endDate.setFullYear(endDate.getFullYear() + warrantyYears);

    const now = new Date();
    if (now >= endDate) return '0 YEARS 0 MONTHS LEFT';

    const diffTime = endDate.getTime() - now.getTime();
    const diffDays = Math.floor(diffTime / (1000 * 60 * 60 * 24));

    const years = Math.floor(diffDays / 365);
    const months = Math.floor((diffDays % 365) / 30);

    return `${years} YEARS ${months} MONTHS LEFT`;
  };

  const calculateWarrantyTimePercentage = (data: InfoVehicleResult): number => {
    const startDateStr = data.vehicle_info?.start_date;
    const warrantyYears = data.vehicle_info?.warranty_date;

    // Vérifier que les données nécessaires sont disponibles
    if (!startDateStr || warrantyYears === undefined || warrantyYears === null) {
      return 0; // Pour la barre de progression, on retourne 0 si les données sont manquantes
    }

    const startDate = new Date(startDateStr);

    // Vérifier que la date est valide
    if (isNaN(startDate.getTime())) {
      return 0;
    }

    const endDate = new Date(startDate);
    endDate.setFullYear(endDate.getFullYear() + warrantyYears);

    const now = new Date();
    if (now >= endDate) return 0;

    const totalTime = endDate.getTime() - startDate.getTime();
    const elapsedTime = now.getTime() - startDate.getTime();
    const remainingTime = totalTime - elapsedTime;
    const percentage = (remainingTime / totalTime) * 100;
    return Math.max(0, Math.min(100, percentage));
  };

  const getExpectedRangeAtWarrantyEnd = (
    vehicleBatteryInfo: InfoVehicleResult,
  ): { range: number | string; percentage: number | string } => {
    const baseRange = vehicleBatteryInfo.battery_info?.range;
    const currentSoh = vehicleBatteryInfo.battery_info?.soh;

    // Si les valeurs nécessaires sont manquantes
    if (
      baseRange === undefined ||
      baseRange === null ||
      baseRange === 0 ||
      currentSoh === undefined ||
      currentSoh === null ||
      currentSoh === 0
    ) {
      return { range: NO_DATA, percentage: NO_DATA };
    }

    // Calculer l'autonomie avec le SoH actuel
    const expectedRange = Math.round((baseRange * currentSoh) / 100);
    return { range: expectedRange, percentage: Math.round(currentSoh) };
  };

  return (
    <div className="w-full md:w-1/2 bg-white rounded-xl p-4 md:p-6 shadow-xs">
      <div className="flex items-center space-x-2 mb-4">
        <div className="w-6 h-6 rounded-full flex items-center justify-center">
          <IconShieldCheck size={24} className="text-green-rapport" />
        </div>
        <h3 className="font-medium">Battery Warranty</h3>
      </div>

      <p className="text-xs mb-4">
        EVs come with two manufacturer warranties: the vehicle warranty and the battery
        warranty which ensures the battery maintains a certain level of capacity and
        performance.
      </p>

      <div className="space-y-6">
        <div>
          <div className="flex justify-between">
            <p className="text-xs text-gray/70 font-medium mb-1">
              REMAINING WARRANTY MILEAGE
            </p>
            <p className="text-xs font-medium whitespace-nowrap">
              {getWarrantyRemaining(vehicleBatteryInfo)}
            </p>
          </div>

          <div className="h-2 bg-gray/50 rounded-full">
            <div
              className="h-2 bg-green-rapport rounded-full"
              style={{
                width: `${calculateWarrantyKmPercentage(vehicleBatteryInfo)}%`,
                transition: 'width 0.5s ease-in-out',
              }}
            ></div>
          </div>
        </div>

        <div>
          <div className="flex justify-between">
            <p className="text-xs text-gray/70 font-medium mb-1">
              REMAINING WARRANTY TIME
            </p>
            <p className="text-xs font-medium whitespace-nowrap">
              {getWarrantyTimeRemaining(vehicleBatteryInfo)}
            </p>
          </div>

          <div className="h-2 bg-gray/50 rounded-full">
            <div
              className="h-2 bg-green-rapport rounded-full"
              style={{
                width: `${calculateWarrantyTimePercentage(vehicleBatteryInfo)}%`,
                transition: 'width 0.5s ease-in-out',
              }}
            ></div>
          </div>
        </div>
      </div>

      <div className="mt-6 mb-4">
        <div className="bg-green-rapport-extra-light/10 rounded-md py-3 px-2 flex justify-between">
          <p className="text-xs font-medium">EXPECTED RANGE AT WARRANTY END</p>
          <p className="text-xs font-medium text-green-rapport">
            {(() => {
              const { range, percentage } =
                getExpectedRangeAtWarrantyEnd(vehicleBatteryInfo);
              if (range === NO_DATA || percentage === NO_DATA) {
                return 'Missing data';
              }
              return `${range} KM (${percentage}%)`;
            })()}
          </p>
        </div>
      </div>

      <p className="text-xs font-medium">
        The original battery warranty guarantees at least 70% State of Health (SoH) for{' '}
        {vehicleBatteryInfo.vehicle_info?.warranty_date || 8} years or{' '}
        {formatNumber(vehicleBatteryInfo.vehicle_info?.warranty_km)} km, whichever comes
        first.
      </p>
    </div>
  );
};

export default WarrantyInfo;
