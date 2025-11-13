import { NO_DATA } from '@/utils/formatNumber';

export const CHARGING_DATA = [
  {
    type: 'Slow',
    color: '#2E7D32',
    outlet: 'Normal outlet',
    current: 'DC',
    power: {
      label: '7kW',
      value: 7,
    },
  },
  {
    type: 'Medium',
    color: '#4CAF50',
    outlet: 'Wall box',
    current: 'DC',
    power: {
      label: '22kW',
      value: 22,
    },
  },
  {
    type: 'Fast',
    color: '#81C784',
    outlet: 'Public outlet',
    current: 'DC',
    power: {
      label: '50kW',
      value: 50,
    },
  },
  {
    type: 'Fastest',
    color: '#C2C2C2',
    outlet: 'Supercharger',
    current: 'AC',
    power: {
      label: '150kW',
      value: 150,
    },
  },
];

const ELECTRICITY_PRICES = {
  Slow: 0.1696,
  Medium: 0.3,
  Fast: 0.495,
  Fastest: 0.605,
};

export const getCostForType = (type: string, capacity: number | undefined): string => {
  if (!capacity) return NO_DATA;

  const effectiveCapacity = capacity * ((80 - 10) * 0.01);
  const electricityPrice = ELECTRICITY_PRICES[type as keyof typeof ELECTRICITY_PRICES];

  const cost = effectiveCapacity * electricityPrice;

  return cost ? `${cost.toFixed(2)} €` : NO_DATA;
};

export const getChargeDuration = (
  power: number,
  capacity: number | undefined,
): string => {
  if (!capacity) return NO_DATA;
  const hours = (capacity * ((80 - 10) * 0.01)) / power;

  if (hours < 1) {
    return `${Math.round(hours * 60)} min`;
  } else {
    const fullHours = Math.floor(hours);
    const minutes = Math.round((hours - fullHours) * 60);
    return minutes > 0 ? `${fullHours}h ${minutes}min` : `${fullHours}h`;
  }
};

// This is used for progress bars
export const getChargeDurationValue = (
  power: number,
  capacity: number | undefined,
): number => {
  return capacity ? (capacity * ((80 - 10) * 0.01)) / power : 0;
};
