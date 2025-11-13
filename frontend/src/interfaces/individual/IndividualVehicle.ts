export type IndividualVehiclePinned = {
  data: {
    vin: string;
    startDate: string;
    soh: number;
    sohPer10000km: number;
    isPinned: boolean;
    timestamp: string;
    odometer: number;
    medianSohLost10000km: number;
  }[];
  page: number | null;
  limit: number | null;
  isLoading: boolean;
  error: unknown;
};

export type IndividualVehicleFastCharge = {
  data: {
    vin: string;
    fastChargeRatio: number;
    totalFastCharge: number;
    totalNormalCharge: number;
    startDate: string;
    odometer: number;
    timestamp: string;
    soh: number;
  }[];
  page: number | null;
  limit: number | null;
  isLoading: boolean;
  error: unknown;
};

export type IndividualVehicleConsumption = {
  data: {
    vin: string;
    consumption: number;
    startDate: string;
    odometer: number;
    timestamp: string;
    soh: number;
  }[];
  page: number | null;
  limit: number | null;
  isLoading: boolean;
  error: unknown;
};
