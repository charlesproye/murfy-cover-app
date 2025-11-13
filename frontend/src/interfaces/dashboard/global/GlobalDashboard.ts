export type GlobalDashboardResult = {
  fleet_id: string;
  avg_soh: number;
  avg_odometer: number;
  vehicle_count: number;
  fleet_name: string;
  region_name: string;
  oem_name: string;
  brands: string;
  regions: string;
};

export type FleetBrandData = {
  [fleetName: string]: {
    [brandName: string]: Array<{
      vin: string;
      odometer: number;
      soh: number;
    }>;
  };
};

export type GlobalScatterRequestSwr = {
  data?: FleetBrandData;
  fleetSelected: boolean;
  isLoading: boolean;
  error: unknown;
};

export type GlobalRegionRequestSwr = {
  data?: FleetBrandData;
  isLoading: boolean;
  error: unknown;
};

export type GlobalScatterResult = {
  vin: string;
  odometer: number;
  soh: number;
};
