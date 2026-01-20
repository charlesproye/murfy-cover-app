import { LanguageEnum } from '@/components/entities/flash-report/forms/schema';

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
    image_url: string | null;
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
    trendline_bib?: string;
    trendline_bib_min?: string;
    trendline_bib_max?: string;
    trendline_oem?: string;
    trendline_oem_min?: string;
    trendline_oem_max?: string;
    soh_bib?: number;
    soh_oem?: number;
  };
}
