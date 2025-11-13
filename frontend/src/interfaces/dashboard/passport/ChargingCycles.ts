export interface ChargingCyclesData {
  level_1_count: number;
  level_2_count: number;
  level_3_count: number;
  total_count: number;
}

export interface ChargingCyclesResponse {
  data: ChargingCyclesData;
}

export interface ChargingCyclesRequestSwr {
  data: ChargingCyclesData | undefined;
  isLoading: boolean;
  error?: Error;
}
