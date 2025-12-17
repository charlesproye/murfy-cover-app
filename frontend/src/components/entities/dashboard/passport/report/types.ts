import { Score } from '@/interfaces/common/score';

export interface VehicleData {
  vin?: string;
  brand?: string;
  model?: string;
  mileage?: number;
  score?: Score;
  soh?: number;
  image?: string;
}
