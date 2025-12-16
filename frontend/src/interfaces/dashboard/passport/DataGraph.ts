import { Score } from './ScoreCard/ScoreCardProps';

export interface DataPoint {
  soh: number;
  odometer: number;
}

export interface DataGraphResponse {
  initial_point: DataPoint;
  data_points: DataPoint[];
  trendline_min: string | null;
  trendline_max: string | null;
  trendline: string | null;
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
