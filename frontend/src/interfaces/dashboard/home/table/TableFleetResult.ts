export type FleetDataItem = {
  vin: string;
  oem_name: string;
  soh: number;
  odometer: number;
};

export type Pagination = {
  current_page: number;
  total_pages: number;
  total_items: number;
};

export type TableFleetResult = {
  data: FleetDataItem[];
  pagination: Pagination;
};
