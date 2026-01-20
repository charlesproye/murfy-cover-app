export interface DataPoint {
  soh_bib: number;
  soh_oem: number;
  odometer: number;
}

export interface DataGraphResponse {
  initial_point: DataPoint;
  data_points: DataPoint[];
  trendline_bib: string | null;
  trendline_bib_min: string | null;
  trendline_bib_max: string | null;
  trendline_oem: string | null;
  trendline_oem_min: string | null;
  trendline_oem_max: string | null;
}

export interface DataGraphRequestSwr {
  data: DataGraphResponse | undefined;
  isLoading: boolean;
  error: unknown;
}

export interface PinVehicleResponse {
  data:
    | {
        is_pinned: boolean;
      }
    | undefined;
  isLoading: boolean;
  error: unknown;
  mutate: () => void;
}
