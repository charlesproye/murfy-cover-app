export type DataPdfResult = {
  id: string;
  licence_plate: string;
  vin: string;
  model_name: string;
  oem_name: string;
  type: string;
  url_image: string;
  start_date: string;
  end_of_contract_date: string;
  warranty_date: string;
  warranty_km: number;
  warranty_remaining: number;
  remaining_warranty_km: number;
  remaining_warranty_days: number;
  autonomy: number;
  base_autonomy: number;
  remaining_range: number;
  capacity: number;
  consumption: number;
  cycles: number;
  soh: number;
  score?: 'A' | 'B' | 'C' | 'D' | 'E' | '';
  odometer: number;
  predictions: {
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
};

export type DataPdfRequestSwr = {
  data: DataPdfResult | undefined;
  isLoading: boolean;
  error: unknown;
};
