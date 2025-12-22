export type Brand = {
  oem_id: string;
  oem_name: string;
  trendline: string;
  trendline_max: string;
  trendline_min: string;
};

export type FilterBrandsProps = {
  changeBrand: (brand: string) => void;
  selectedBrand: string | '';
  brands: Brand[];
  data: TrendlineData[];
};

export interface TrendlineData {
  soh: number;
  odometer: number;
  vin: string;
  in_fleet: boolean;
  score: string;
  [key: string]: unknown;
}

export interface TrendlineEquation {
  brand: Brand;
  trendline: string;
  trendline_max: string;
  trendline_min: string;
}

export interface TrendlineChartProps {
  data: TrendlineData[];
  selectedBrand?: string;
  trendlineEquations?: TrendlineEquation[];
}

export type DashboardDataTrendline = {
  brands: Brand[];
  trendline: TrendlineData[];
};

export type ResponseTrendline = {
  data?: DashboardDataTrendline;
  isLoading: boolean;
  error: unknown;
};

export type GraphTrendlineProps = {
  fleet: string;
};
