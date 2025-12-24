import { Score } from '@/interfaces/common/score';

interface FetchProps {
  isLoading: boolean;
  error: unknown;
}

export interface InfoVehicleResult {
  vehicle_info: {
    vin: string;
    brand: string;
    model: string;
    odometer: number;
    score: Score;
    start_date: string;
    image_url: string | null;
    licence_plate: string;
    warranty_km: number;
    warranty_date: number;
    cycles: number;
  };
  battery_info: {
    oem: string;
    chemistry: string;
    capacity: number;
    range: number;
    consumption: number;
    soh: number;
    trendline: string | null;
  };
  end_of_contract_date?: string;
  last_data_date?: string;
  activation_status?: boolean;
}

// Type pour la réponse API qui peut avoir des valeurs manquantes
export interface ApiVehicleResponse {
  vehicle_info?: {
    vin?: string;
    brand?: string;
    model?: string;
    mileage?: number;
    score?: Score;
    start_date?: string;
  };
  licence_plate?: string;
  end_of_contract_date?: string;
  soh?: number;
  odometer?: number;
  battery_info?: {
    oem?: string;
    chemistry?: string;
    capacity?: number;
    range?: number;
    consumption?: number;
  };
}

export type InfoVehicleRequestSwr = FetchProps & {
  data: InfoVehicleResult | undefined;
};

export type PriceForecastRequestSwr = FetchProps & {
  data?: PriceForecastResult | undefined;
};

export type PriceForecastResult = {
  price: number | null;
  price_discount: number | null;
};

export type EstimatedRangeRequestSwr = FetchProps & {
  data: EstimatedRangeResult | undefined;
};

export type RangeBarChartProps = {
  data: EstimatedRangeResult | undefined;
};

export type KpiAdditionalRequestSwr = FetchProps & {
  data: KpiAdditionalResult | undefined;
};

export type KpiAdditionalResult = {
  cycles: number;
  consumption: number;
};

export type EstimatedRangeResult = {
  initial: {
    soh: number;
    range_min: number;
    range_max: number;
    odometer: number;
  };
  current: {
    soh: number;
    range_min: number;
    range_max: number;
    odometer: number;
  };
  predictions: Array<{
    soh: number;
    range_min: number;
    range_max: number;
    odometer: number;
  }>;
};
