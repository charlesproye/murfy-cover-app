import { LanguageEnum } from '@/components/flash-report/forms/schema';

export interface GetGenerationInfo {
  has_trendline: boolean;
  language: LanguageEnum;
  vehicle_info: {
    vin: string;
    brand: string;
    model: string;
    type: string | null;
    version: string | null;
    mileage: number;
    image_url: string;
    warranty_date: number;
    warranty_km: number;
  };
  battery_info: {
    oem?: string;
    chemistry: string;
    net_capacity: number;
    capacity: number;
    consumption?: number;
    range: number;
    trendline?: string;
    trendline_min?: string;
    trendline_max?: string;
    soh?: number;
  };
}
