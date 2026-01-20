export type TableBrandsResult = {
  oem_id?: string;
  oem_name?: string;
  avg_odometer?: number;
  avg_soh?: number;
  nb_vehicle?: number;
  region_name?: string;
  region_id?: string;
};

export type OEM = {
  oem_id: string;
  oem_name: string;
};

export type TableExtremumResult = {
  vehicles: {
    vin: string;
    oem_name: string;
    odometer: number;
    soh: number | null;
    soh_oem: number | null;
    years_remaining?: number;
    score: string;
    consumption?: number;
    fast_charge_ratio?: number;
  }[];
  brands: OEM[];
  pagination: {
    page: number;
    page_size: number;
    total_items: number;
    total_pages: number;
    has_next: boolean;
    has_previous: boolean;
  };
};
