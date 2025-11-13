import { Score } from './ScoreCard/ScoreCardProps';

export interface DataPoint {
  soh_vehicle: number;
  timestamp: string | null;
  odometer: number;
  model_name: string;
  is_current_vehicle: boolean;
  start_date: string | null;
}

export interface DataGraphResponse {
  initial_point: DataPoint;
  data_points: DataPoint[];
  trendline_min: string;
  trendline_max: string;
  trendline: string;
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
}

export type DataCardGraphResult = {
  title: string;
  data: number[] | Score[];
  url_image: string;
};

export type DataCardGraphRequestSwr = {
  data: DataCardGraphResult[] | undefined;
  isLoading: boolean;
  error: unknown;
};
