import { Score } from '@/interfaces/dashboard/passport/ScoreCard/ScoreCardProps';

export interface VehicleData {
  vin?: string;
  brand?: string;
  model?: string;
  mileage?: number;
  score?: Score;
  soh?: number;
  image?: string;
}
