export type TableBrandsResult = {
  oem_id?: string;
  oem_name?: string;
  avg_odometer?: number;
  avg_soh?: number;
  nb_vehicle?: number;
  region_name?: string;
  region_id?: string;
};

export type TableExtremumResult = {
  vehicles: {
    vin: string;
    oem_name: string;
    odometer: number;
    soh: number;
    years_remaining?: number;
    score: string;
  }[];
  brands: {
    oem_id: string;
    oem_name: string;
  }[];
  pagination: {
    page: number;
    page_size: number;
    total_items: number;
    total_pages: number;
    has_next: boolean;
    has_previous: boolean;
  };
};
